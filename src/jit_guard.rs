use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct JitGuardCfg {
    pub tx_start_delay_ms: u32,
    pub tx_jit_delay_ms: u32,
    pub post_tx_rx_guard_ms: u32,
}

impl Default for JitGuardCfg {
    fn default() -> Self {
        Self {
            tx_start_delay_ms: 2,
            tx_jit_delay_ms: 80,
            post_tx_rx_guard_ms: 10,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledTx {
    pub start: Instant,
    pub end: Instant,
}

#[derive(Debug)]
pub struct JitGuard {
    cfg: JitGuardCfg,
    last_tx: Option<ScheduledTx>,
}

#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error("tx scheduled too late")]
    TooLate,
    #[error("post tx guard violation")]
    GuardViolation,
}

impl JitGuard {
    pub fn new(cfg: JitGuardCfg) -> Self {
        Self { cfg, last_tx: None }
    }

    pub fn schedule_tx(
        &mut self,
        now: Instant,
        airtime_us: u32,
    ) -> Result<ScheduledTx, ScheduleError> {
        let start = now + Duration::from_millis(self.cfg.tx_start_delay_ms as u64);
        let end = start + Duration::from_micros(airtime_us as u64);

        if let Some(last) = &self.last_tx {
            let guard = Duration::from_millis(self.cfg.post_tx_rx_guard_ms as u64);
            if now < last.end + guard {
                return Err(ScheduleError::GuardViolation);
            }
        }

        self.last_tx = Some(ScheduledTx { start, end });
        Ok(self.last_tx.clone().unwrap())
    }

    pub fn on_tx_done(&mut self, end: Instant) {
        if let Some(last) = &mut self.last_tx {
            last.end = end;
        }
    }

    pub fn can_enter_rx(&self, now: Instant) -> bool {
        match &self.last_tx {
            None => true,
            Some(last) => {
                let guard = Duration::from_millis(self.cfg.post_tx_rx_guard_ms as u64);
                now >= last.end + guard
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_blocks_back_to_back() {
        let cfg = JitGuardCfg {
            tx_start_delay_ms: 1,
            tx_jit_delay_ms: 80,
            post_tx_rx_guard_ms: 10,
        };
        let mut guard = JitGuard::new(cfg);
        let now = Instant::now();
        let _ = guard.schedule_tx(now, 1000).unwrap();
        assert!(guard
            .schedule_tx(now + Duration::from_millis(5), 1000)
            .is_err());
    }
}
