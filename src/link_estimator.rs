use std::collections::HashMap;
use std::hash::Hash;

use crate::airtime::{toa_us, AirtimeKey, LoraPhy};

/// Snapshot of the current link statistics for a neighbour.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkStats {
    pub prr_in: f32,
    pub prr_out: f32,
    pub etx: f32,
    pub toa_us: u32,
    pub atx: f32,
}

impl Default for LinkStats {
    fn default() -> Self {
        Self {
            prr_in: 1.0,
            prr_out: 1.0,
            etx: 1.0,
            toa_us: 0,
            atx: 1.0,
        }
    }
}

/// Trait representing a link estimator implementation.
pub trait LinkEstimator<N>
where
    N: Eq + Hash + Copy,
{
    fn observe_tx(&mut self, nhop: N, uplink_id: u16);
    fn observe_ack(&mut self, nhop: N, uplink_id: u16, ok: bool);
    fn snapshot(&self, nhop: N, payload_len: usize, phy: &LoraPhy) -> LinkStats;
}

#[derive(Debug, Clone, Copy)]
pub struct LinkEstimatorCfg {
    pub ewma_alpha: f32,
    pub min_prr: f32,
}

impl Default for LinkEstimatorCfg {
    fn default() -> Self {
        Self {
            ewma_alpha: 0.25,
            min_prr: 0.05,
        }
    }
}

#[derive(Debug, Clone)]
struct EwmaEntry {
    prr_in: f32,
    prr_out: f32,
    tx_total: u32,
    ack_total: u32,
}

impl Default for EwmaEntry {
    fn default() -> Self {
        Self {
            prr_in: 1.0,
            prr_out: 1.0,
            tx_total: 0,
            ack_total: 0,
        }
    }
}

/// Basic EWMA based link estimator.
pub struct EwmaLinkEstimator<N>
where
    N: Eq + Hash + Copy,
{
    cfg: LinkEstimatorCfg,
    entries: HashMap<N, EwmaEntry>,
}

impl<N> EwmaLinkEstimator<N>
where
    N: Eq + Hash + Copy,
{
    pub fn new(cfg: LinkEstimatorCfg) -> Self {
        Self {
            cfg,
            entries: HashMap::new(),
        }
    }

    fn entry_mut(&mut self, nhop: N) -> &mut EwmaEntry {
        self.entries.entry(nhop).or_default()
    }

    fn entry(&self, nhop: N) -> EwmaEntry {
        self.entries.get(&nhop).cloned().unwrap_or_default()
    }
}

impl<N> LinkEstimator<N> for EwmaLinkEstimator<N>
where
    N: Eq + Hash + Copy,
{
    fn observe_tx(&mut self, nhop: N, _uplink_id: u16) {
        let alpha = self.cfg.ewma_alpha;
        let entry = self.entry_mut(nhop);
        entry.tx_total = entry.tx_total.saturating_add(1);
        entry.prr_out = (1.0 - alpha) * entry.prr_out;
    }

    fn observe_ack(&mut self, nhop: N, _uplink_id: u16, ok: bool) {
        let alpha = self.cfg.ewma_alpha;
        let entry = self.entry_mut(nhop);
        entry.ack_total = entry.ack_total.saturating_add(1);
        if ok {
            entry.prr_out = (1.0 - alpha) * entry.prr_out + alpha;
            entry.prr_in = (1.0 - alpha) * entry.prr_in + alpha;
        } else {
            entry.prr_out *= 1.0 - alpha;
        }
    }

    fn snapshot(&self, nhop: N, payload_len: usize, phy: &LoraPhy) -> LinkStats {
        let entry = self.entry(nhop);
        let prr_in = entry.prr_in.max(self.cfg.min_prr);
        let prr_out = entry.prr_out.max(self.cfg.min_prr);
        let etx = 1.0 / (prr_in * prr_out);
        let toa_us = toa_us(&AirtimeKey {
            phy: *phy,
            payload_len,
        });
        let atx = etx * (toa_us as f32 / 1_000_000.0);

        LinkStats {
            prr_in,
            prr_out,
            etx,
            toa_us,
            atx,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_converges() {
        let phy = LoraPhy {
            sf: 12,
            cr: (4, 8),
            bw_hz: 812_000,
        };
        let mut estimator: EwmaLinkEstimator<u8> = EwmaLinkEstimator::new(LinkEstimatorCfg {
            ewma_alpha: 0.5,
            min_prr: 0.01,
        });

        for _ in 0..10 {
            estimator.observe_tx(1, 1);
            estimator.observe_ack(1, 1, true);
        }

        let stats = estimator.snapshot(1, 30, &phy);
        assert!(stats.prr_in > 0.5);
        assert!(stats.prr_out > 0.5);
        assert!(stats.etx <= 4.0);
        assert!(stats.atx > 0.0);
    }

    #[test]
    fn snapshot_uses_min_prr() {
        let phy = LoraPhy {
            sf: 12,
            cr: (4, 8),
            bw_hz: 812_000,
        };
        let estimator: EwmaLinkEstimator<u8> = EwmaLinkEstimator::new(LinkEstimatorCfg {
            ewma_alpha: 0.5,
            min_prr: 0.2,
        });

        let stats = estimator.snapshot(1, 30, &phy);
        assert_eq!(stats.prr_in, 1.0);
        assert_eq!(stats.prr_out, 1.0);
        assert!(stats.etx >= 1.0);
    }
}
