use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckLite {
    pub child_relay_id: u8,
    pub uplink_id: u16,
    pub status: AckStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    Failed,
}

impl AckLite {
    pub const LEN: usize = 4;

    pub fn encode(&self) -> [u8; Self::LEN] {
        let mut buf = [0u8; Self::LEN];
        buf[0] = self.child_relay_id;
        buf[1..3].copy_from_slice(&self.uplink_id.to_be_bytes());
        buf[3] = match self.status {
            AckStatus::Ok => 1,
            AckStatus::Failed => 0,
        };
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != Self::LEN {
            return Err(anyhow!("invalid ack-lite length: {}", data.len()));
        }
        let uplink_id = u16::from_be_bytes([data[1], data[2]]);
        let status = match data[3] {
            0 => AckStatus::Failed,
            1 => AckStatus::Ok,
            v => return Err(anyhow!("invalid ack-lite status: {}", v)),
        };
        Ok(Self {
            child_relay_id: data[0],
            uplink_id,
            status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let ack = AckLite {
            child_relay_id: 0x0A,
            uplink_id: 0x1234,
            status: AckStatus::Ok,
        };

        let buf = ack.encode();
        let decoded = AckLite::decode(&buf).unwrap();
        assert_eq!(ack, decoded);
    }

    #[test]
    fn decode_rejects_invalid_len() {
        assert!(AckLite::decode(&[1, 2, 3]).is_err());
    }
}
