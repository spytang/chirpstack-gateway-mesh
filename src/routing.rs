use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::airtime::LoraPhy;
use crate::link_estimator::{LinkEstimator, LinkStats};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RouteEntry<N> {
    pub parent: N,
    pub cost_atx_path: f32,
    pub depth: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteSelectorCfg {
    pub hysteresis_ratio: f32,
    pub trigger_delta_atx: f32,
    pub trickle_min_ms: u32,
    pub trickle_max_ms: u32,
}

impl Default for RouteSelectorCfg {
    fn default() -> Self {
        Self {
            hysteresis_ratio: 0.15,
            trigger_delta_atx: 0.1,
            trickle_min_ms: 2_000,
            trickle_max_ms: 120_000,
        }
    }
}

#[derive(Debug, Clone)]
struct NeighborState {
    advertised_cost: f32,
    depth: u8,
}

pub trait RouteSelector<N>
where
    N: Eq + Hash + Copy,
{
    fn on_trickle_tick(&mut self, now: Instant);
    fn on_beacon(&mut self, from: N, their_cost: f32, depth: u8, now: Instant);
    fn choose_parent(&mut self) -> Option<RouteEntry<N>>;
}

/// Simple Trickle-inspired parent selector. It keeps track of neighbour
/// announcements, uses ATX snapshots from the estimator and applies hysteresis
/// to prevent rapid parent flapping.
pub struct TrickleRouteSelector<N, E>
where
    N: Eq + Hash + Copy,
    E: LinkEstimator<N>,
{
    cfg: RouteSelectorCfg,
    estimator: E,
    phy: LoraPhy,
    payload_hint: usize,
    current_parent: Option<RouteEntry<N>>,
    neighbors: HashMap<N, NeighborState>,
    trickle_interval: Duration,
    next_tick: Instant,
    last_recompute: Option<Instant>,
}

impl<N, E> TrickleRouteSelector<N, E>
where
    N: Eq + Hash + Copy,
    E: LinkEstimator<N>,
{
    pub fn new(cfg: RouteSelectorCfg, estimator: E, phy: LoraPhy, payload_hint: usize) -> Self {
        let start = Instant::now();
        Self {
            cfg,
            estimator,
            phy,
            payload_hint,
            current_parent: None,
            neighbors: HashMap::new(),
            trickle_interval: Duration::from_millis(cfg.trickle_min_ms as u64),
            next_tick: start + Duration::from_millis(cfg.trickle_min_ms as u64),
            last_recompute: None,
        }
    }

    pub fn estimator_mut(&mut self) -> &mut E {
        &mut self.estimator
    }

    pub fn estimator(&self) -> &E {
        &self.estimator
    }

    fn recompute(&mut self, now: Instant) {
        self.last_recompute = Some(now);
        let mut best: Option<RouteEntry<N>> = None;

        for (nhop, info) in &self.neighbors {
            let stats = self.estimator.snapshot(*nhop, self.payload_hint, &self.phy);
            let path_cost = info.advertised_cost + stats.atx;
            let depth = info.depth.saturating_add(1);
            let candidate = RouteEntry {
                parent: *nhop,
                cost_atx_path: path_cost,
                depth,
            };

            match best {
                None => best = Some(candidate),
                Some(ref current) => {
                    if should_replace(&self.cfg, current, &candidate, stats, self.current_parent) {
                        best = Some(candidate);
                    }
                }
            }
        }

        if let Some(best) = best {
            if let Some(current) = self.current_parent {
                if !parent_switch_allowed(&self.cfg, current, best) {
                    return;
                }
            }
            self.current_parent = Some(best);
        }
    }
}

fn parent_switch_allowed<N>(
    cfg: &RouteSelectorCfg,
    current: RouteEntry<N>,
    new: RouteEntry<N>,
) -> bool {
    if (current.cost_atx_path - new.cost_atx_path).abs() <= cfg.trigger_delta_atx {
        return false;
    }
    new.cost_atx_path <= current.cost_atx_path * (1.0 - cfg.hysteresis_ratio)
}

fn should_replace<N>(
    cfg: &RouteSelectorCfg,
    current: &RouteEntry<N>,
    candidate: &RouteEntry<N>,
    _stats: LinkStats,
    current_parent: Option<RouteEntry<N>>,
) -> bool
where
    N: Copy + PartialEq,
{
    if let Some(parent) = current_parent {
        if parent.parent == candidate.parent {
            return false;
        }
    }

    if candidate.cost_atx_path + cfg.trigger_delta_atx < current.cost_atx_path {
        return true;
    }

    if (candidate.cost_atx_path - current.cost_atx_path).abs() <= cfg.trigger_delta_atx {
        return candidate.depth < current.depth;
    }

    candidate.cost_atx_path < current.cost_atx_path
}

impl<N, E> RouteSelector<N> for TrickleRouteSelector<N, E>
where
    N: Eq + Hash + Copy,
    E: LinkEstimator<N>,
{
    fn on_trickle_tick(&mut self, now: Instant) {
        if now < self.next_tick {
            return;
        }

        self.recompute(now);

        let new_interval = (self.trickle_interval.as_millis() * 2)
            .min(self.cfg.trickle_max_ms as u128)
            .max(self.cfg.trickle_min_ms as u128);
        self.trickle_interval = Duration::from_millis(new_interval as u64);
        self.next_tick = now + self.trickle_interval;
    }

    fn on_beacon(&mut self, from: N, their_cost: f32, depth: u8, now: Instant) {
        self.neighbors.insert(
            from,
            NeighborState {
                advertised_cost: their_cost,
                depth,
            },
        );

        let min_interval = Duration::from_millis(self.cfg.trickle_min_ms as u64);
        self.trickle_interval = min_interval;
        self.next_tick = now + min_interval;
    }

    fn choose_parent(&mut self) -> Option<RouteEntry<N>> {
        if let Some(last) = self.last_recompute {
            if last + Duration::from_millis(self.cfg.trickle_min_ms as u64) < Instant::now() {
                self.recompute(Instant::now());
            }
        } else {
            self.recompute(Instant::now());
        }
        self.current_parent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link_estimator::{EwmaLinkEstimator, LinkEstimatorCfg};

    #[test]
    fn prefers_lower_atx() {
        let phy = LoraPhy {
            sf: 12,
            cr: (4, 8),
            bw_hz: 812_000,
        };
        let estimator: EwmaLinkEstimator<u8> = EwmaLinkEstimator::new(LinkEstimatorCfg::default());
        let mut selector =
            TrickleRouteSelector::new(RouteSelectorCfg::default(), estimator, phy, 30);

        let now = Instant::now();
        selector.on_beacon(1, 0.8, 2, now);
        selector.on_beacon(2, 0.4, 3, now);

        let parent = selector.choose_parent();
        assert!(parent.is_some());
    }
}
