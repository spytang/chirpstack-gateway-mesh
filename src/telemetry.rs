use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct LinkTelemetry {
    pub sf: Option<u8>,
    pub cr: Option<String>,
    pub bw_hz: Option<u32>,
    pub toa_us: Option<u32>,
    pub etx: Option<f32>,
    pub atx_link: Option<f32>,
    pub atx_path: Option<f32>,
    pub parent_id: Option<String>,
    pub ack_count: Option<u32>,
    pub mesh_delay_ms: Option<u32>,
    pub uplink_id: Option<u32>,
}

impl LinkTelemetry {
    pub fn apply_to(&self, metadata: &mut BTreeMap<String, String>) {
        if let Some(v) = self.sf {
            metadata.insert("sf".to_string(), v.to_string());
        }
        if let Some(v) = &self.cr {
            metadata.insert("cr".to_string(), v.clone());
        }
        if let Some(v) = self.bw_hz {
            metadata.insert("bw_hz".to_string(), v.to_string());
        }
        if let Some(v) = self.toa_us {
            metadata.insert("toa_us".to_string(), v.to_string());
        }
        if let Some(v) = self.etx {
            metadata.insert("etx".to_string(), format!("{:.3}", v));
        }
        if let Some(v) = self.atx_link {
            metadata.insert("atx_link".to_string(), format!("{:.3}", v));
        }
        if let Some(v) = self.atx_path {
            metadata.insert("atx_path".to_string(), format!("{:.3}", v));
        }
        if let Some(v) = &self.parent_id {
            metadata.insert("parent_id".to_string(), v.clone());
        }
        if let Some(v) = self.ack_count {
            metadata.insert("ack_count".to_string(), v.to_string());
        }
        if let Some(v) = self.mesh_delay_ms {
            metadata.insert("mesh_delay_ms".to_string(), v.to_string());
        }
        if let Some(v) = self.uplink_id {
            metadata.insert("uplink_id".to_string(), v.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_insertion() {
        let mut meta = BTreeMap::new();
        LinkTelemetry {
            sf: Some(12),
            cr: Some("4/8".into()),
            bw_hz: Some(812_000),
            toa_us: Some(425_000),
            etx: Some(1.23),
            atx_link: Some(0.52),
            atx_path: Some(1.73),
            parent_id: Some("0x03".into()),
            ack_count: Some(4),
            mesh_delay_ms: Some(600),
            uplink_id: Some(417),
        }
        .apply_to(&mut meta);

        assert_eq!(meta.get("sf"), Some(&"12".to_string()));
        assert_eq!(meta.get("cr"), Some(&"4/8".to_string()));
        assert_eq!(meta.get("parent_id"), Some(&"0x03".to_string()));
    }
}
