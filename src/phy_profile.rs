use std::collections::HashMap;
use std::time::Instant;

use crate::airtime::{toa_us, AirtimeKey, LoraPhy};

#[derive(Debug, Clone)]
pub struct PhyProfile {
    pub allowed: Vec<LoraPhy>,
}

impl Default for PhyProfile {
    fn default() -> Self {
        Self {
            allowed: vec![LoraPhy {
                sf: 12,
                cr: (4, 8),
                bw_hz: 812_000,
            }],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub phy: LoraPhy,
    pub success: bool,
    pub timestamp: Instant,
}

#[derive(Debug)]
pub struct SfProbeCache {
    entries: HashMap<(u32, LoraPhy), ProbeResult>,
}

impl SfProbeCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn update(&mut self, relay: u32, phy: LoraPhy, success: bool) {
        self.entries.insert(
            (relay, phy),
            ProbeResult {
                phy,
                success,
                timestamp: Instant::now(),
            },
        );
    }

    pub fn best_for(&self, relay: u32, payload_len: usize, profile: &PhyProfile) -> LoraPhy {
        let mut best = profile.allowed.first().copied().unwrap_or(LoraPhy {
            sf: 12,
            cr: (4, 8),
            bw_hz: 812_000,
        });
        let mut best_cost = f32::MAX;

        for phy in &profile.allowed {
            if let Some(res) = self.entries.get(&(relay, *phy)) {
                if !res.success {
                    continue;
                }
            }

            let key = AirtimeKey {
                phy: *phy,
                payload_len,
            };
            let toa = toa_us(&key) as f32;
            if toa < best_cost {
                best_cost = toa;
                best = *phy;
            }
        }

        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chooses_fastest_phy() {
        let profile = PhyProfile {
            allowed: vec![
                LoraPhy {
                    sf: 12,
                    cr: (4, 8),
                    bw_hz: 812_000,
                },
                LoraPhy {
                    sf: 7,
                    cr: (4, 5),
                    bw_hz: 812_000,
                },
            ],
        };
        let mut cache = SfProbeCache::new();
        cache.update(1, profile.allowed[0], true);
        cache.update(1, profile.allowed[1], true);

        let best = cache.best_for(1, 30, &profile);
        assert_eq!(best.sf, 7);
    }
}
