use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// Description of a LoRa physical layer profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LoraPhy {
    pub sf: u8,
    pub cr: (u8, u8),
    pub bw_hz: u32,
}

/// Key used for airtime cache lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AirtimeKey {
    pub phy: LoraPhy,
    pub payload_len: usize,
}

static AIRTIME_CACHE: LazyLock<Mutex<HashMap<AirtimeKey, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Returns the airtime (time-on-air) in microseconds for the given key.
///
/// The computation follows the LoRa modulation time-on-air formula as
/// documented in the Semtech datasheets. Results are cached to avoid
/// repeatedly evaluating the expensive floating point expressions.
pub fn toa_us(key: &AirtimeKey) -> u32 {
    if let Some(v) = AIRTIME_CACHE.lock().unwrap().get(key) {
        return *v;
    }

    let toa = compute_toa_us(key);
    AIRTIME_CACHE
        .lock()
        .unwrap()
        .entry(key.clone())
        .or_insert(toa);
    toa
}

fn compute_toa_us(key: &AirtimeKey) -> u32 {
    let sf = key.phy.sf as f32;
    let bw = key.phy.bw_hz as f32;
    let (cr_num, cr_den) = key.phy.cr;
    let cr = (cr_den.saturating_sub(cr_num)) as f32; // CR = denominator - numerator, e.g. 4/5 -> 1

    // Low data-rate optimization as defined by the LoRa spec.
    let low_dr_opt = if (key.phy.bw_hz == 125_000 && key.phy.sf >= 11)
        || (key.phy.bw_hz == 62_500 && key.phy.sf >= 10)
    {
        1.0
    } else {
        0.0
    };

    let symbol_duration = 2f32.powi(sf as i32) / bw; // seconds
    let preamble_symb = 8.0_f32 + 4.25; // default preamble of 8 symbols + 4.25

    let payload_symb_nb = {
        let payload = key.payload_len as f32;
        let numerator = 8.0 * payload - 4.0 * sf + 28.0 + 16.0 - 20.0 * low_dr_opt;
        let denominator = 4.0 * (sf - 2.0 * low_dr_opt);
        let ceil = (numerator / denominator).ceil().max(0.0);
        8.0_f32.max(ceil * (cr + 4.0))
    };

    let total_time_s = (preamble_symb + payload_symb_nb) * symbol_duration;
    (total_time_s * 1_000_000.0) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toa_cache() {
        let phy = LoraPhy {
            sf: 12,
            cr: (4, 8),
            bw_hz: 812_000,
        };
        let key = AirtimeKey {
            phy,
            payload_len: 47,
        };

        let v1 = toa_us(&key);
        let v2 = toa_us(&key);
        assert_eq!(v1, v2);
        assert!(v1 > 0);
    }

    #[test]
    fn test_toa_changes_with_payload() {
        let phy = LoraPhy {
            sf: 7,
            cr: (4, 5),
            bw_hz: 125_000,
        };
        let short = AirtimeKey {
            phy,
            payload_len: 1,
        };
        let long = AirtimeKey {
            phy,
            payload_len: 50,
        };

        assert!(toa_us(&long) > toa_us(&short));
    }
}
