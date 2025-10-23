use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use rand::Rng;
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;

use crate::packets::MeshPacket;
use crate::{backend, config, helpers};

use super::{ctp, get_mesh_frequency};

#[derive(Clone)]
pub struct TxPlan {
    pub data_rate: config::DataRate,
    pub toa: Duration,
}

pub struct ScheduledPacket {
    pub packet: MeshPacket,
    pub plan: TxPlan,
    pub flow_id: [u8; 4],
    pub next_hop: Option<[u8; 4]>,
    pub requires_ack: bool,
}

struct FlowQueue {
    pending: Option<ScheduledPacket>,
    scheduled: bool,
}

struct SchedulerState {
    flows: HashMap<[u8; 4], FlowQueue>,
    order: VecDeque<[u8; 4]>,
}

impl SchedulerState {
    fn new() -> Self {
        SchedulerState {
            flows: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn push(&mut self, packet: ScheduledPacket) {
        let flow_id = packet.flow_id;
        let entry = self.flows.entry(flow_id).or_insert_with(|| FlowQueue {
            pending: None,
            scheduled: false,
        });
        entry.pending = Some(packet);
        if !entry.scheduled {
            entry.scheduled = true;
            self.order.push_back(flow_id);
        }
    }

    fn pop_next(&mut self) -> Option<ScheduledPacket> {
        while let Some(flow_id) = self.order.pop_front() {
            if let Some(mut queue) = self.flows.remove(&flow_id) {
                queue.scheduled = false;
                if let Some(packet) = queue.pending.take() {
                    return Some(packet);
                }
            }
        }
        None
    }
}

struct Scheduler {
    inner: Mutex<SchedulerState>,
    notify: Notify,
}

static SCHEDULER: Lazy<Scheduler> = Lazy::new(|| Scheduler {
    inner: Mutex::new(SchedulerState::new()),
    notify: Notify::new(),
});

static START: OnceLock<()> = OnceLock::new();

pub const BEACON_FLOW_ID: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

pub fn init() {
    START.get_or_init(|| {
        tokio::spawn(async {
            worker_loop().await;
        });
    });
}

pub async fn enqueue(
    packet: MeshPacket,
    plan: TxPlan,
    flow_id: [u8; 4],
    next_hop: Option<[u8; 4]>,
    requires_ack: bool,
) {
    let scheduled = ScheduledPacket {
        packet,
        plan,
        flow_id,
        next_hop,
        requires_ack,
    };

    let mut state = SCHEDULER.inner.lock().await;
    state.push(scheduled);
    drop(state);
    SCHEDULER.notify.notify_one();
}

async fn worker_loop() {
    loop {
        let packet = {
            let mut state = SCHEDULER.inner.lock().await;
            state.pop_next()
        };

        match packet {
            Some(packet) => {
                if let Err(e) = transmit(packet).await {
                    warn!("Failed to transmit mesh packet via scheduler, error: {}", e);
                }
            }
            None => {
                SCHEDULER.notify.notified().await;
            }
        }
    }
}

async fn transmit(packet: ScheduledPacket) -> Result<()> {
    let ScheduledPacket {
        packet,
        plan,
        flow_id: _,
        next_hop,
        requires_ack,
    } = packet;

    if let Some(parent) = next_hop {
        ctp::record_tx_attempt(parent, &plan).await;
    }

    let res = send_packet(packet.clone(), &plan).await;

    if let Some(parent) = next_hop {
        ctp::record_tx_result(parent, &plan, res.is_ok()).await;
    }

    res?;

    if requires_ack {
        apply_backoff(&plan.toa).await;
    }

    Ok(())
}

async fn send_packet(packet: MeshPacket, plan: &TxPlan) -> Result<()> {
    let conf = config::get();
    let frequency = get_mesh_frequency(&conf)?;
    let modulation = helpers::data_rate_to_gw_modulation(&plan.data_rate, false);

    let pl = chirpstack_api::gw::DownlinkFrame {
        downlink_id: rand::random(),
        items: vec![chirpstack_api::gw::DownlinkFrameItem {
            phy_payload: packet.to_vec()?,
            tx_info: Some(chirpstack_api::gw::DownlinkTxInfo {
                frequency,
                power: conf.mesh.tx_power,
                modulation: Some(modulation),
                timing: Some(chirpstack_api::gw::Timing {
                    parameters: Some(chirpstack_api::gw::timing::Parameters::Immediately(
                        chirpstack_api::gw::ImmediatelyTimingInfo {},
                    )),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    info!(
        "Scheduled mesh transmission, downlink_id: {}, toa_ms: {}",
        pl.downlink_id,
        plan.toa.as_millis()
    );

    backend::mesh(pl).await
}

async fn apply_backoff(toa: &Duration) {
    if toa.is_zero() {
        return;
    }

    let base = toa.as_secs_f64();
    if base == 0.0 {
        return;
    }

    let mut rng = rand::thread_rng();
    let factor = rng.gen_range(1.5..=2.5);
    let sleep_time = Duration::from_secs_f64(base * factor);
    debug!(
        "Applying CTP backoff, duration_ms: {}",
        sleep_time.as_millis()
    );
    sleep(sleep_time).await;
}
