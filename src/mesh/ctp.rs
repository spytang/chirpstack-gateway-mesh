use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{debug, trace, warn};
use once_cell::sync::{Lazy, OnceCell};
use tokio::sync::{Mutex, Notify};

use crate::backend;
use crate::config::{Configuration, CtpConfig, DataRate};
use crate::packets::{
    Metric, RouteInfo, RoutingBeaconFlags, RoutingBeaconPayload, RoutingNeighborEntry,
};

use super::scheduler::TxPlan;

const MIN_SF: u8 = 7;
const MAX_SF: u8 = 12;

const TOA_TABLE: &[(u8, u16)] = &[(7, 13), (8, 21), (9, 44), (10, 82), (11, 152), (12, 304)];

#[allow(dead_code)]
#[derive(Clone)]
pub struct RoutingDecision {
    pub parent: [u8; 4],
    pub data_rate: DataRate,
    pub toa: Duration,
    pub path_metric: f32,
    pub link_metric: f32,
    pub forward_delivery: f32,
    pub link_etx: f32,
}

impl RoutingDecision {
    pub fn plan(&self) -> TxPlan {
        TxPlan {
            data_rate: self.data_rate.clone(),
            toa: self.toa,
        }
    }
}

struct NeighborState {
    id: [u8; 4],
    last_seen: Instant,
    neighbor_metric: f32,
    parent: Option<[u8; 4]>,
    reverse_delivery: f32,
    forward_delivery: f32,
    sf: u8,
    toa: Duration,
}

struct ParentInfo {
    parent: [u8; 4],
    data_rate: DataRate,
    toa: Duration,
    link_metric: f32,
    link_etx: f32,
    forward_delivery: f32,
}

struct TrickleState {
    current_interval: Duration,
    min_interval: Duration,
    max_interval: Duration,
    force_immediate: bool,
    pending_pull: bool,
}

impl TrickleState {
    fn new(conf: &CtpConfig) -> Self {
        TrickleState {
            current_interval: conf.beacon_min_interval,
            min_interval: conf.beacon_min_interval,
            max_interval: conf.beacon_max_interval,
            force_immediate: true,
            pending_pull: false,
        }
    }

    fn reset(&mut self) {
        self.current_interval = self.min_interval;
        self.force_immediate = true;
    }

    fn after_send(&mut self) {
        let next = self.current_interval.saturating_add(self.current_interval);
        self.current_interval = if next > self.max_interval {
            self.max_interval
        } else {
            next
        };
        self.force_immediate = false;
        self.pending_pull = false;
    }
}

struct RoutingState {
    relay_id: [u8; 4],
    border_gateway: bool,
    config: CtpConfig,
    base_rate: DataRate,
    neighbors: HashMap<[u8; 4], NeighborState>,
    parent: Option<ParentInfo>,
    path_metric: f32,
    trickle: TrickleState,
}

struct ParentUpdate {
    changed: bool,
    improved: bool,
}

impl ParentUpdate {
    fn none() -> Self {
        ParentUpdate {
            changed: false,
            improved: false,
        }
    }

    fn should_reset(&self) -> bool {
        self.changed || self.improved
    }
}

impl RoutingState {
    fn new(relay_id: [u8; 4], border_gateway: bool, conf: &Configuration) -> Self {
        let config = conf.mesh.ctp.clone();
        let base_rate = conf.mesh.data_rate.clone();
        let path_metric = if border_gateway { 0.0 } else { f32::INFINITY };

        RoutingState {
            relay_id,
            border_gateway,
            config,
            base_rate,
            neighbors: HashMap::new(),
            parent: None,
            path_metric,
            trickle: TrickleState::new(&conf.mesh.ctp),
        }
    }

    fn set_pending_pull(&mut self) {
        self.trickle.pending_pull = true;
        self.trickle.force_immediate = true;
    }

    fn recompute_parent(&mut self, now: Instant) -> ParentUpdate {
        if self.border_gateway {
            self.parent = None;
            self.path_metric = 0.0;
            return ParentUpdate::none();
        }

        let mut removed_parent = false;
        let expiration = self.config.neighbor_expiration;
        self.neighbors.retain(|id, n| {
            if now.duration_since(n.last_seen) > expiration {
                if self
                    .parent
                    .as_ref()
                    .map(|p| &p.parent == id)
                    .unwrap_or(false)
                {
                    removed_parent = true;
                }
                false
            } else {
                true
            }
        });

        let prev_parent = self.parent.as_ref().map(|p| p.parent);
        let prev_metric = self.path_metric;

        let mut best: Option<ParentInfo> = None;
        let mut best_metric = f32::INFINITY;

        for neighbor in self.neighbors.values() {
            let link = compute_link_etx(neighbor.forward_delivery, neighbor.reverse_delivery);
            let toa_ms = toa_ms(neighbor.toa);
            let total_metric = neighbor.neighbor_metric + link * toa_ms;

            if total_metric < best_metric {
                let mut rate = self.base_rate.clone();
                rate.spreading_factor = neighbor.sf;
                best_metric = total_metric;
                best = Some(ParentInfo {
                    parent: neighbor.id,
                    data_rate: rate,
                    toa: neighbor.toa,
                    link_metric: link * toa_ms,
                    link_etx: link,
                    forward_delivery: neighbor.forward_delivery,
                });
            }
        }

        self.parent = best;
        self.path_metric = best_metric;

        if let Some(parent) = &self.parent {
            debug!(
                "Selected parent: {}, metric: {:.3}",
                hex::encode(parent.parent),
                best_metric
            );
        }

        let changed = prev_parent != self.parent.as_ref().map(|p| p.parent) || removed_parent;
        let improved = prev_metric.is_finite()
            && best_metric.is_finite()
            && prev_metric / best_metric >= self.config.cost_improvement_reset;

        ParentUpdate { changed, improved }
    }

    fn current_decision(&self) -> Option<RoutingDecision> {
        if let Some(parent) = &self.parent {
            Some(RoutingDecision {
                parent: parent.parent,
                data_rate: parent.data_rate.clone(),
                toa: parent.toa,
                path_metric: self.path_metric,
                link_metric: parent.link_metric,
                forward_delivery: parent.forward_delivery,
                link_etx: parent.link_etx,
            })
        } else {
            None
        }
    }

    fn update_neighbor_from_beacon(
        &mut self,
        relay_id: [u8; 4],
        beacon: &RoutingBeaconPayload,
        now: Instant,
    ) {
        let entry = self
            .neighbors
            .entry(relay_id)
            .or_insert_with(|| NeighborState::new(relay_id, &self.base_rate));

        entry.last_seen = now;
        entry.neighbor_metric = beacon.path_metric.to_f32();
        entry.parent = beacon.parent;

        if let Some(item) = beacon
            .neighbors
            .iter()
            .find(|n| n.neighbor_id == self.relay_id)
        {
            entry.reverse_delivery = item.delivery().clamp(0.01, 1.0);
            entry.sf = item.spreading_factor.clamp(MIN_SF, MAX_SF);
            entry.toa = toa_for_sf(entry.sf);
        }
    }

    fn build_beacon(&mut self) -> RoutingBeaconPayload {
        let mut flags = RoutingBeaconFlags::empty();
        if self.trickle.pending_pull {
            flags = flags.union(RoutingBeaconFlags::PULL);
        }

        let mut neighbors = Vec::new();
        for neighbor in self.neighbors.values() {
            neighbors.push(RoutingNeighborEntry::with_delivery(
                neighbor.id,
                neighbor.forward_delivery,
                neighbor.sf,
            ));
        }

        RoutingBeaconPayload {
            flags,
            parent: self.parent.as_ref().map(|p| p.parent),
            path_metric: Metric::from_f32(self.path_metric),
            neighbors,
        }
    }
}

impl NeighborState {
    fn new(id: [u8; 4], base_rate: &DataRate) -> Self {
        let sf = base_rate.spreading_factor.clamp(MIN_SF, MAX_SF);
        NeighborState {
            id,
            last_seen: Instant::now(),
            neighbor_metric: f32::INFINITY,
            parent: None,
            reverse_delivery: 0.5,
            forward_delivery: 0.5,
            sf,
            toa: toa_for_sf(sf),
        }
    }
}

static ROUTING_STATE: Lazy<Mutex<RoutingState>> =
    Lazy::new(|| Mutex::new(RoutingState::new([0; 4], false, &Configuration::default())));

static BEACON_NOTIFY: Lazy<Notify> = Lazy::new(Notify::new);
static BEACON_LOOP_STARTED: OnceCell<()> = OnceCell::new();

pub async fn setup(conf: &Configuration) -> Result<()> {
    let relay_id = backend::get_relay_id().await?;
    {
        let mut state = ROUTING_STATE.lock().await;
        *state = RoutingState::new(relay_id, conf.mesh.border_gateway, conf);
        if !conf.mesh.border_gateway {
            state.set_pending_pull();
        }
    }

    BEACON_LOOP_STARTED.get_or_init(|| {
        tokio::spawn(async {
            beacon_loop().await;
        });
        tokio::spawn(async {
            cleanup_loop().await;
        });
    });

    BEACON_NOTIFY.notify_one();

    Ok(())
}

pub async fn prepare_uplink() -> Option<RoutingDecision> {
    let mut state = ROUTING_STATE.lock().await;
    let update = state.recompute_parent(Instant::now());
    let decision = state.current_decision();
    drop(state);

    if update.should_reset() {
        reset_trickle("parent update").await;
    }

    decision
}

pub async fn prepare_forward(_child_id: [u8; 4], _child_metric: f32) -> Option<RoutingDecision> {
    let mut state = ROUTING_STATE.lock().await;
    let update = state.recompute_parent(Instant::now());
    let decision = state.current_decision();
    drop(state);

    if update.should_reset() {
        reset_trickle("parent update").await;
    }

    decision
}

pub async fn record_tx_attempt(parent: [u8; 4], plan: &TxPlan) {
    let mut state = ROUTING_STATE.lock().await;
    if let Some(neighbor) = state.neighbors.get_mut(&parent) {
        neighbor.sf = plan.data_rate.spreading_factor.clamp(MIN_SF, MAX_SF);
        neighbor.toa = plan.toa;
    }
}

pub async fn record_tx_result(parent: [u8; 4], plan: &TxPlan, success: bool) {
    let mut state = ROUTING_STATE.lock().await;
    let alpha = state.config.ewma_alpha.clamp(0.01, 1.0);
    let sample = if success { 1.0 } else { 0.0 };

    if let Some(neighbor) = state.neighbors.get_mut(&parent) {
        neighbor.forward_delivery = (1.0 - alpha) * neighbor.forward_delivery + alpha * sample;
        neighbor.forward_delivery = neighbor.forward_delivery.clamp(0.01, 1.0);
        neighbor.sf = plan.data_rate.spreading_factor.clamp(MIN_SF, MAX_SF);
        neighbor.toa = plan.toa;

        if !success && neighbor.sf < MAX_SF {
            neighbor.sf += 1;
            neighbor.toa = toa_for_sf(neighbor.sf);
        } else if success && neighbor.forward_delivery > 0.95 && neighbor.sf > MIN_SF {
            neighbor.sf -= 1;
            neighbor.toa = toa_for_sf(neighbor.sf);
        }
    }
    let update = state.recompute_parent(Instant::now());
    drop(state);

    if update.should_reset() {
        reset_trickle("link metric update").await;
    }
}

pub async fn handle_beacon(relay_id: [u8; 4], beacon: &RoutingBeaconPayload) {
    let mut state = ROUTING_STATE.lock().await;
    let now = Instant::now();
    state.update_neighbor_from_beacon(relay_id, beacon, now);
    let update = state.recompute_parent(now);
    drop(state);

    if beacon.flags.contains(RoutingBeaconFlags::PULL) {
        reset_trickle("received pull").await;
    } else if update.should_reset() {
        reset_trickle("beacon parent update").await;
    }
}

pub async fn notify_pull_request() {
    {
        let mut state = ROUTING_STATE.lock().await;
        state.set_pending_pull();
    }
    reset_trickle("pull request").await;
}

pub async fn notify_inconsistency(child_metric: f32) {
    let path_metric = {
        let state = ROUTING_STATE.lock().await;
        state.path_metric
    };

    if path_metric.is_finite() && child_metric + 1.0 < path_metric {
        reset_trickle("route inconsistency").await;
    }
}

#[allow(dead_code)]
pub async fn current_path_metric() -> f32 {
    let state = ROUTING_STATE.lock().await;
    state.path_metric
}

#[allow(dead_code)]
pub async fn current_route_info() -> RouteInfo {
    let state = ROUTING_STATE.lock().await;
    if let Some(parent) = &state.parent {
        RouteInfo {
            parent: Some(parent.parent),
            path_metric: Metric::from_f32(state.path_metric),
            link_metric: Metric::from_f32(parent.link_metric),
            link_toa_ms: parent.toa.as_millis() as u16,
        }
    } else {
        RouteInfo::default()
    }
}

async fn reset_trickle(reason: &str) {
    {
        let mut state = ROUTING_STATE.lock().await;
        state.trickle.reset();
    }
    debug!("Resetting trickle timer, reason: {}", reason);
    BEACON_NOTIFY.notify_one();
}

async fn beacon_loop() {
    loop {
        let immediate_payload = {
            let mut state = ROUTING_STATE.lock().await;
            if state.trickle.force_immediate {
                let payload = state.build_beacon();
                state.trickle.after_send();
                Some(payload)
            } else {
                None
            }
        };

        if let Some(payload) = immediate_payload {
            if let Err(e) = super::enqueue_beacon(payload).await {
                warn!("Failed to enqueue beacon: {}", e);
            }
            continue;
        }

        let interval = {
            let state = ROUTING_STATE.lock().await;
            state.trickle.current_interval
        };

        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                let payload = {
                    let mut state = ROUTING_STATE.lock().await;
                    let payload = state.build_beacon();
                    state.trickle.after_send();
                    payload
                };

                if let Err(e) = super::enqueue_beacon(payload).await {
                    warn!("Failed to enqueue beacon: {}", e);
                }
            }
            _ = BEACON_NOTIFY.notified() => {
                trace!("Beacon loop notified");
            }
        }
    }
}

async fn cleanup_loop() {
    loop {
        let interval = {
            let state = ROUTING_STATE.lock().await;
            state.config.neighbor_expiration / 2
        };

        tokio::time::sleep(interval).await;

        let update = {
            let mut state = ROUTING_STATE.lock().await;
            state.recompute_parent(Instant::now())
        };

        if update.should_reset() {
            reset_trickle("cleanup parent update").await;
        }
    }
}

pub(super) fn toa_for_sf(sf: u8) -> Duration {
    let sf = sf.clamp(MIN_SF, MAX_SF);
    let ms = TOA_TABLE
        .iter()
        .find(|(s, _)| *s == sf)
        .map(|(_, v)| *v)
        .unwrap_or(TOA_TABLE.last().map(|(_, v)| *v).unwrap_or(304));
    Duration::from_millis(ms as u64)
}

fn toa_ms(duration: Duration) -> f32 {
    duration.as_secs_f32() * 1000.0
}

fn compute_link_etx(forward: f32, reverse: f32) -> f32 {
    let forward = forward.clamp(0.01, 1.0);
    let reverse = reverse.clamp(0.01, 1.0);
    1.0 / (forward * reverse)
}
