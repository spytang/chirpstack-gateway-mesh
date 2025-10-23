use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;

use anyhow::Result;
use chirpstack_api::{gw, prost_types};
use log::{debug, info, trace, warn};
use rand::random;

mod ctp;
mod scheduler;

use crate::{
    aes128::{get_encryption_key, get_signing_key, Aes128Key},
    backend,
    cache::{Cache, PayloadCache},
    commands,
    config::{self, Configuration},
    events, helpers,
    packets::{
        self, DownlinkMetadata, Event, EventPayload, MeshPacket, Metric, Payload, PayloadType,
        RouteInfo, RoutingBeaconPayload, UplinkMetadata, UplinkPayload, MHDR,
    },
    proxy,
};

use scheduler::TxPlan;

static CTX_PREFIX: [u8; 3] = [1, 2, 3];
static MESH_CHANNEL: Mutex<usize> = Mutex::new(0);
static UPLINK_ID: Mutex<u16> = Mutex::new(0);
static UPLINK_CONTEXT: LazyLock<Mutex<HashMap<u16, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PAYLOAD_CACHE: LazyLock<Mutex<Cache<PayloadCache>>> =
    LazyLock::new(|| Mutex::new(Cache::new(64)));

pub async fn setup(conf: &Configuration) -> Result<()> {
    scheduler::init();
    ctp::setup(conf).await
}

// Handle LoRaWAN payload (non-proprietary).
pub async fn handle_uplink(border_gateway: bool, pl: &gw::UplinkFrame) -> Result<()> {
    match border_gateway {
        true => proxy_uplink_lora_packet(pl).await,
        false => relay_uplink_lora_packet(pl).await,
    }
}

// Handle Proprietary LoRaWAN payload (mesh encapsulated).
pub async fn handle_mesh(border_gateway: bool, pl: &gw::UplinkFrame) -> Result<()> {
    let conf = config::get();
    let mut packet = MeshPacket::from_slice(&pl.phy_payload)?;
    if !packet.validate_mic(if conf.mesh.signing_key != Aes128Key::null() {
        conf.mesh.signing_key
    } else {
        get_signing_key(conf.mesh.root_key)
    })? {
        warn!("Dropping packet, invalid MIC, mesh_packet: {}", packet);
        return Ok(());
    }

    // If we can't add the packet to the cache, it means we have already seen the packet and we can
    // drop it.
    if !PAYLOAD_CACHE.lock().unwrap().add((&packet).into()) {
        trace!(
            "Dropping packet as it has already been seen, mesh_packet: {}",
            packet
        );
        return Ok(());
    };

    // Decrypt the packet (in case it contains an encrypted payload).
    packet.decrypt(get_encryption_key(conf.mesh.root_key))?;

    match border_gateway {
        // Proxy relayed uplink
        true => match packet.mhdr.payload_type {
            PayloadType::Uplink => proxy_uplink_mesh_packet(pl, packet).await,
            PayloadType::Event => proxy_event_mesh_packet(pl, packet).await,
            _ => Ok(()),
        },
        false => relay_mesh_packet(pl, packet).await,
    }
}

pub async fn handle_downlink(pl: gw::DownlinkFrame) -> Result<gw::DownlinkTxAck> {
    if let Some(first_item) = pl.items.first() {
        let tx_info = first_item
            .tx_info
            .as_ref()
            .ok_or_else(|| anyhow!("tx_info is None"))?;

        // Check if context has the CTX_PREFIX, if not we just proxy the downlink payload.
        if tx_info.context.len() != CTX_PREFIX.len() + 6
            || !tx_info.context[0..CTX_PREFIX.len()].eq(&CTX_PREFIX)
        {
            return proxy_downlink_lora_packet(pl).await;
        }
    }

    relay_downlink_lora_packet(&pl).await
}

pub async fn send_mesh_command(pl: gw::MeshCommand) -> Result<()> {
    let conf = config::get();

    let mut packet = packets::MeshPacket {
        mhdr: packets::MHDR {
            payload_type: packets::PayloadType::Command,
            hop_count: 1,
        },
        payload: packets::Payload::Command(packets::CommandPayload {
            timestamp: SystemTime::now(),
            relay_id: {
                let mut relay_id: [u8; 4] = [0; 4];
                hex::decode_to_slice(&pl.relay_id, &mut relay_id)?;
                relay_id
            },
            commands: pl
                .commands
                .iter()
                .filter_map(|v| {
                    v.command
                        .as_ref()
                        .map(|gw::mesh_command_item::Command::Proprietary(v)| {
                            packets::Command::Proprietary((v.command_type as u8, v.payload.clone()))
                        })
                })
                .collect(),
        }),
        mic: None,
    };
    packet.encrypt(get_encryption_key(conf.mesh.root_key))?;
    packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
        conf.mesh.signing_key
    } else {
        get_signing_key(conf.mesh.root_key)
    })?;

    let pl = gw::DownlinkFrame {
        downlink_id: random(),
        items: vec![gw::DownlinkFrameItem {
            phy_payload: packet.to_vec()?,
            tx_info: Some(gw::DownlinkTxInfo {
                frequency: get_mesh_frequency(&conf)?,
                modulation: Some(helpers::data_rate_to_gw_modulation(
                    &conf.mesh.data_rate,
                    false,
                )),
                power: conf.mesh.tx_power,
                timing: Some(gw::Timing {
                    parameters: Some(gw::timing::Parameters::Immediately(
                        gw::ImmediatelyTimingInfo {},
                    )),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    info!(
        "Sending mesh packet, downlink_id: {}, mesh_packet: {}",
        pl.downlink_id, packet
    );
    backend::mesh(pl).await
}

async fn proxy_downlink_lora_packet(pl: gw::DownlinkFrame) -> Result<gw::DownlinkTxAck> {
    info!(
        "Proxying LoRaWAN downlink, downlink: {}",
        helpers::format_downlink(&pl)?
    );
    backend::send_downlink(pl).await
}

async fn proxy_uplink_lora_packet(pl: &gw::UplinkFrame) -> Result<()> {
    info!(
        "Proxying LoRaWAN uplink, uplink: {}",
        helpers::format_uplink(pl)?
    );
    let mut pl = pl.clone();

    if let Some(rx_info) = &mut pl.rx_info {
        rx_info
            .metadata
            .insert("uplink_id".to_string(), rx_info.uplink_id.to_string());
    }

    let pl = gw::Event {
        event: Some(gw::event::Event::UplinkFrame(pl)),
    };

    proxy::send_event(pl).await
}

async fn proxy_uplink_mesh_packet(pl: &gw::UplinkFrame, packet: MeshPacket) -> Result<()> {
    let mesh_pl = match &packet.payload {
        Payload::Uplink(v) => v,
        _ => {
            return Err(anyhow!("Expected Uplink payload"));
        }
    };

    info!(
        "Unwrapping relayed uplink, uplink_id: {}, mesh_packet: {}",
        pl.rx_info.as_ref().map(|v| v.uplink_id).unwrap_or_default(),
        packet
    );

    let mut pl = pl.clone();

    if let Some(rx_info) = &mut pl.rx_info {
        // Set gateway ID.
        rx_info.gateway_id = hex::encode(backend::get_gateway_id().await?);

        // Set uplink ID.
        rx_info.uplink_id = mesh_pl.metadata.uplink_id.into();

        // Set metadata.
        rx_info
            .metadata
            .insert("hop_count".to_string(), (packet.mhdr.hop_count).to_string());
        rx_info
            .metadata
            .insert("relay_id".to_string(), hex::encode(mesh_pl.relay_id));
        rx_info.metadata.insert(
            "uplink_id".to_string(),
            mesh_pl.metadata.uplink_id.to_string(),
        );
        if let Some(parent) = mesh_pl.route.parent {
            rx_info
                .metadata
                .insert("ctp_parent".to_string(), hex::encode(parent));
        }
        rx_info.metadata.insert(
            "ctp_path_metric".to_string(),
            format!("{:.2}", mesh_pl.route.path_metric.to_f32()),
        );
        rx_info.metadata.insert(
            "ctp_link_metric".to_string(),
            format!("{:.2}", mesh_pl.route.link_metric.to_f32()),
        );
        rx_info.metadata.insert(
            "ctp_link_toa_ms".to_string(),
            mesh_pl.route.link_toa_ms.to_string(),
        );

        // Calculate mesh delay (in ms) and add to metadata.
        let delay = helpers::ms_since_midnight().saturating_sub(mesh_pl.timestamp);
        rx_info
            .metadata
            .insert("mesh_delay_ms".to_string(), delay.to_string());

        // Set RSSI and SNR.
        rx_info.snr = mesh_pl.metadata.snr.into();
        rx_info.rssi = mesh_pl.metadata.rssi.into();

        // Set context.
        rx_info.context = {
            let mut ctx = Vec::with_capacity(CTX_PREFIX.len() + 6); // Relay ID = 4 + Uplink ID = 2
            ctx.extend_from_slice(&CTX_PREFIX);
            ctx.extend_from_slice(&mesh_pl.relay_id);
            ctx.extend_from_slice(&mesh_pl.metadata.uplink_id.to_be_bytes());
            ctx
        };
    }

    // Set TxInfo.
    if let Some(tx_info) = &mut pl.tx_info {
        tx_info.frequency = helpers::chan_to_frequency(mesh_pl.metadata.channel)?;
        tx_info.modulation = Some(helpers::dr_to_modulation(mesh_pl.metadata.dr, false)?);
    }

    // Set original PHYPayload.
    pl.phy_payload.clone_from(&mesh_pl.phy_payload);

    let pl = gw::Event {
        event: Some(gw::event::Event::UplinkFrame(pl)),
    };

    proxy::send_event(pl).await
}

async fn proxy_event_mesh_packet(pl: &gw::UplinkFrame, packet: MeshPacket) -> Result<()> {
    let mesh_pl = match &packet.payload {
        Payload::Event(v) => v,
        _ => {
            return Err(anyhow!("Expected Heartbeat payload"));
        }
    };

    info!(
        "Unwrapping relay event packet, uplink_id: {}, mesh_packet: {}",
        pl.rx_info.as_ref().map(|v| v.uplink_id).unwrap_or_default(),
        packet
    );

    let event = gw::Event {
        event: Some(gw::event::Event::Mesh(gw::MeshEvent {
            gateway_id: hex::encode(backend::get_gateway_id().await?),
            relay_id: hex::encode(mesh_pl.relay_id),
            time: Some(mesh_pl.timestamp.into()),
            events: mesh_pl
                .events
                .iter()
                .filter_map(|e| match e {
                    Event::RoutingBeacon(_) => None,
                    Event::Heartbeat(v) => Some(gw::MeshEventItem {
                        event: Some(gw::mesh_event_item::Event::Heartbeat(
                            gw::MeshEventHeartbeat {
                                relay_path: v
                                    .relay_path
                                    .iter()
                                    .map(|v| gw::MeshEventHeartbeatRelayPath {
                                        relay_id: hex::encode(v.relay_id),
                                        rssi: v.rssi.into(),
                                        snr: v.snr.into(),
                                    })
                                    .collect(),
                            },
                        )),
                    }),
                    Event::Proprietary(v) => Some(gw::MeshEventItem {
                        event: Some(gw::mesh_event_item::Event::Proprietary(
                            gw::MeshEventProprietary {
                                event_type: v.0.into(),
                                payload: v.1.clone(),
                            },
                        )),
                    }),
                    Event::Encrypted(_) => panic!("Events must be decrypted first"),
                })
                .collect(),
        })),
    };

    proxy::send_event(event).await?;

    Ok(())
}

async fn relay_mesh_packet(pl: &gw::UplinkFrame, mut packet: MeshPacket) -> Result<()> {
    let conf = config::get();
    let relay_id = backend::get_relay_id().await?;
    let rx_info = pl
        .rx_info
        .as_ref()
        .ok_or_else(|| anyhow!("rx_info is None"))?;

    match &mut packet.payload {
        packets::Payload::Uplink(pl) => {
            if pl.relay_id == relay_id {
                trace!("Dropping packet as this relay was the sender");

                // Drop the packet, as we are the original sender.
                return Ok(());
            }

            let expected_parent = match pl.route.parent {
                Some(parent) => parent,
                None => {
                    trace!("Dropping uplink without parent assignment");
                    return Ok(());
                }
            };

            if expected_parent != relay_id {
                trace!(
                    "Dropping uplink because this relay is not the selected parent, expected_parent: {}",
                    hex::encode(expected_parent)
                );
                return Ok(());
            }

            let child_metric = pl.route.path_metric.to_f32();
            ctp::notify_inconsistency(child_metric).await;

            let decision = match ctp::prepare_forward(pl.relay_id, child_metric).await {
                Some(v) => v,
                None => {
                    warn!(
                        "No upstream parent available for child {}, requesting new beacons",
                        hex::encode(pl.relay_id)
                    );
                    ctp::notify_pull_request().await;
                    return Ok(());
                }
            };

            pl.route.parent = Some(decision.parent);
            pl.route.path_metric = Metric::from_f32(decision.path_metric);
            pl.route.link_metric = Metric::from_f32(decision.link_metric);
            pl.route.link_toa_ms = decision.toa.as_millis() as u16;

            packet.mhdr.hop_count += 1;

            if packet.mhdr.hop_count > conf.mesh.max_hop_count {
                return Err(anyhow!("Max hop count exceeded"));
            }

            packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
                conf.mesh.signing_key
            } else {
                get_signing_key(conf.mesh.root_key)
            })?;

            info!(
                "Forwarding mesh uplink from {}, next_hop: {}, metric: {:.2}",
                hex::encode(pl.relay_id),
                hex::encode(decision.parent),
                decision.path_metric
            );

            scheduler::enqueue(
                packet,
                decision.plan(),
                pl.relay_id,
                Some(decision.parent),
                true,
            )
            .await;

            return Ok(());
        }
        packets::Payload::Downlink(pl) => {
            if pl.relay_id == relay_id {
                // We must unwrap the mesh encapsulated packet and send it to the
                // End Device.

                let pl = gw::DownlinkFrame {
                    downlink_id: random(),
                    items: vec![gw::DownlinkFrameItem {
                        phy_payload: pl.phy_payload.clone(),
                        tx_info: Some(gw::DownlinkTxInfo {
                            frequency: pl.metadata.frequency,
                            power: helpers::index_to_tx_power(pl.metadata.tx_power)?,
                            timing: Some(gw::Timing {
                                parameters: Some(gw::timing::Parameters::Delay(
                                    gw::DelayTimingInfo {
                                        delay: Some(prost_types::Duration {
                                            seconds: pl.metadata.delay.into(),
                                            ..Default::default()
                                        }),
                                    },
                                )),
                            }),
                            modulation: Some(helpers::dr_to_modulation(pl.metadata.dr, true)?),
                            context: get_uplink_context(pl.metadata.uplink_id)?,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    gateway_id: hex::encode(backend::get_gateway_id().await?),
                    ..Default::default()
                };

                info!(
                    "Unwrapping relayed downlink, downlink_id: {}, mesh_packet: {}",
                    pl.downlink_id, packet
                );
                return helpers::tx_ack_to_err(&backend::send_downlink(pl).await?);
            }
        }
        packets::Payload::Event(pl) => {
            if pl.relay_id == relay_id {
                trace!("Dropping packet as this relay was the sender");

                // Drop the packet, as we are the sender.
                return Ok(());
            }

            let mut events_to_forward = Vec::new();
            for event in pl.events.drain(..) {
                match event {
                    Event::Heartbeat(mut hb) => {
                        hb.relay_path.push(packets::RelayPath {
                            relay_id,
                            rssi: rx_info.rssi as i16,
                            snr: rx_info.snr as i8,
                        });
                        events_to_forward.push(Event::Heartbeat(hb));
                    }
                    Event::RoutingBeacon(beacon) => {
                        ctp::handle_beacon(pl.relay_id, &beacon).await;
                    }
                    other => {
                        events_to_forward.push(other);
                    }
                }
            }

            pl.events = events_to_forward;

            if pl.events.is_empty() {
                trace!("Dropping event packet as there is nothing to forward");
                return Ok(());
            }
        }
        packets::Payload::Command(pl) => {
            if pl.relay_id == relay_id {
                // The command payload was intended for this gateway, execute
                // the commands.
                let resp = commands::execute_commands(pl).await?;

                // Send back the responses (events).
                if !resp.is_empty() {
                    events::send_events(resp).await?;
                }

                return Ok(());
            }
        }
    }

    // In any other case, we increment the hop_count and re-transmit the mesh encapsulated
    // packet.

    // Increment hop count.
    packet.mhdr.hop_count += 1;

    // Encrypt.
    packet.encrypt(get_encryption_key(conf.mesh.root_key))?;

    // We need to re-set the MIC as we have changed the payload by incrementing
    // the hop count (and in casee of heartbeat, we have modified the Relay path).
    packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
        conf.mesh.signing_key
    } else {
        get_signing_key(conf.mesh.root_key)
    })?;

    if packet.mhdr.hop_count > conf.mesh.max_hop_count {
        return Err(anyhow!("Max hop count exceeded"));
    }

    let pl = gw::DownlinkFrame {
        downlink_id: random(),
        items: vec![gw::DownlinkFrameItem {
            phy_payload: packet.to_vec()?,
            tx_info: Some(gw::DownlinkTxInfo {
                frequency: get_mesh_frequency(&conf)?,
                modulation: Some(helpers::data_rate_to_gw_modulation(
                    &conf.mesh.data_rate,
                    false,
                )),
                power: conf.mesh.tx_power,
                timing: Some(gw::Timing {
                    parameters: Some(gw::timing::Parameters::Immediately(
                        gw::ImmediatelyTimingInfo {},
                    )),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    info!(
        "Re-relaying mesh packet, downlink_id: {}, mesh_packet: {}",
        pl.downlink_id, packet
    );
    backend::mesh(pl).await
}

async fn relay_uplink_lora_packet(pl: &gw::UplinkFrame) -> Result<()> {
    let conf = config::get();

    let rx_info = pl
        .rx_info
        .as_ref()
        .ok_or_else(|| anyhow!("rx_info is None"))?;
    let tx_info = pl
        .tx_info
        .as_ref()
        .ok_or_else(|| anyhow!("tx_info is None"))?;
    let modulation = tx_info
        .modulation
        .as_ref()
        .ok_or_else(|| anyhow!("modulation is None"))?;

    let decision = match ctp::prepare_uplink().await {
        Some(v) => v,
        None => {
            warn!(
                "No routing parent available, requesting neighbors to advertise, uplink_id: {}",
                rx_info.uplink_id
            );
            ctp::notify_pull_request().await;
            return Ok(());
        }
    };

    let relay_id = backend::get_relay_id().await?;

    let mut packet = MeshPacket {
        mhdr: MHDR {
            payload_type: PayloadType::Uplink,
            hop_count: 1,
        },
        payload: Payload::Uplink(UplinkPayload {
            metadata: UplinkMetadata {
                uplink_id: store_uplink_context(&rx_info.context),
                dr: helpers::modulation_to_dr(modulation)?,
                channel: helpers::frequency_to_chan(tx_info.frequency)?,
                rssi: rx_info.rssi as i16,
                snr: rx_info.snr as i8,
            },
            timestamp: helpers::ms_since_midnight(),
            relay_id,
            route: RouteInfo {
                parent: Some(decision.parent),
                path_metric: Metric::from_f32(decision.path_metric),
                link_metric: Metric::from_f32(decision.link_metric),
                link_toa_ms: decision.toa.as_millis() as u16,
            },
            phy_payload: pl.phy_payload.clone(),
        }),
        mic: None,
    };
    packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
        conf.mesh.signing_key
    } else {
        get_signing_key(conf.mesh.root_key)
    })?;
    info!(
        "Scheduling uplink for mesh forwarding, uplink_id: {}, parent: {}, metric: {:.2}",
        rx_info.uplink_id,
        hex::encode(decision.parent),
        decision.path_metric
    );

    scheduler::enqueue(
        packet,
        decision.plan(),
        relay_id,
        Some(decision.parent),
        true,
    )
    .await;

    Ok(())
}

async fn relay_downlink_lora_packet(pl: &gw::DownlinkFrame) -> Result<gw::DownlinkTxAck> {
    let conf = config::get();

    let mut tx_ack_items: Vec<gw::DownlinkTxAckItem> = pl
        .items
        .iter()
        .map(|_| gw::DownlinkTxAckItem {
            status: gw::TxAckStatus::Ignored.into(),
        })
        .collect();

    for (i, downlink_item) in pl.items.iter().enumerate() {
        let tx_info = downlink_item
            .tx_info
            .as_ref()
            .ok_or_else(|| anyhow!("tx_info is None"))?;
        let modulation = tx_info
            .modulation
            .as_ref()
            .ok_or_else(|| anyhow!("modulation is None"))?;
        let timing = tx_info
            .timing
            .as_ref()
            .ok_or_else(|| anyhow!("timing is None"))?;
        let delay = match &timing.parameters {
            Some(gw::timing::Parameters::Delay(v)) => v
                .delay
                .as_ref()
                .map(|v| v.seconds as u8)
                .unwrap_or_default(),
            _ => {
                return Err(anyhow!("Only Delay timing is supported"));
            }
        };

        let ctx = tx_info
            .context
            .get(CTX_PREFIX.len()..CTX_PREFIX.len() + 6)
            .ok_or_else(|| anyhow!("context does not contain enough bytes"))?;

        let mut packet = packets::MeshPacket {
            mhdr: packets::MHDR {
                payload_type: packets::PayloadType::Downlink,
                hop_count: 1,
            },
            payload: packets::Payload::Downlink(packets::DownlinkPayload {
                phy_payload: downlink_item.phy_payload.clone(),
                relay_id: {
                    let mut b: [u8; 4] = [0; 4];
                    b.copy_from_slice(&ctx[0..4]);
                    b
                },
                metadata: DownlinkMetadata {
                    uplink_id: {
                        let mut b: [u8; 2] = [0; 2];
                        b.copy_from_slice(&ctx[4..6]);
                        u16::from_be_bytes(b)
                    },
                    dr: helpers::modulation_to_dr(modulation)?,
                    frequency: tx_info.frequency,
                    tx_power: helpers::tx_power_to_index(tx_info.power)?,
                    delay,
                },
            }),
            mic: None,
        };
        packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
            conf.mesh.signing_key
        } else {
            get_signing_key(conf.mesh.root_key)
        })?;

        let pl = gw::DownlinkFrame {
            downlink_id: pl.downlink_id,
            items: vec![gw::DownlinkFrameItem {
                phy_payload: packet.to_vec()?,
                tx_info: Some(gw::DownlinkTxInfo {
                    frequency: get_mesh_frequency(&conf)?,
                    power: conf.mesh.tx_power,
                    modulation: Some(helpers::data_rate_to_gw_modulation(
                        &conf.mesh.data_rate,
                        false,
                    )),
                    timing: Some(gw::Timing {
                        parameters: Some(gw::timing::Parameters::Immediately(
                            gw::ImmediatelyTimingInfo {},
                        )),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        info!(
            "Sending downlink frame as relayed downlink, downlink_id: {}, mesh_packet: {}",
            pl.downlink_id, packet
        );

        match backend::mesh(pl).await {
            Ok(_) => {
                tx_ack_items[i].status = gw::TxAckStatus::Ok.into();
                break;
            }
            Err(e) => {
                warn!("Relay downlink failed, error: {}", e);
                tx_ack_items[i].status = gw::TxAckStatus::InternalError.into();
            }
        }
    }

    Ok(gw::DownlinkTxAck {
        gateway_id: pl.gateway_id.clone(),
        downlink_id: pl.downlink_id,
        items: tx_ack_items,
        ..Default::default()
    })
}

pub fn get_mesh_frequency(conf: &Configuration) -> Result<u32> {
    if conf.mesh.frequencies.is_empty() {
        return Err(anyhow!("No mesh frequencies are configured"));
    }

    let mut mesh_channel = MESH_CHANNEL.lock().unwrap();
    *mesh_channel += 1;

    if *mesh_channel >= conf.mesh.frequencies.len() {
        *mesh_channel = 0;
    }

    Ok(conf.mesh.frequencies[*mesh_channel])
}

fn get_uplink_id() -> u16 {
    let mut uplink_id = UPLINK_ID.lock().unwrap();
    *uplink_id += 1;

    if *uplink_id > 4095 {
        *uplink_id = 0;
    }

    *uplink_id
}

pub fn store_uplink_context(ctx: &[u8]) -> u16 {
    let uplink_id = get_uplink_id();
    let mut uplink_ctx = UPLINK_CONTEXT.lock().unwrap();
    uplink_ctx.insert(uplink_id, ctx.to_vec());
    uplink_id
}

fn get_uplink_context(uplink_id: u16) -> Result<Vec<u8>> {
    let uplink_ctx = UPLINK_CONTEXT.lock().unwrap();
    uplink_ctx
        .get(&uplink_id)
        .cloned()
        .ok_or_else(|| anyhow!("No uplink context for uplink_id: {}", uplink_id))
}

pub(super) async fn enqueue_beacon(payload: RoutingBeaconPayload) -> Result<()> {
    let conf = config::get();
    let relay_id = backend::get_relay_id().await?;

    let mut event_payload = EventPayload {
        timestamp: SystemTime::now(),
        relay_id,
        events: vec![Event::RoutingBeacon(payload)],
    };

    let mut packet = MeshPacket {
        mhdr: MHDR {
            payload_type: PayloadType::Event,
            hop_count: 1,
        },
        payload: Payload::Event(event_payload),
        mic: None,
    };

    packet.set_mic(if conf.mesh.signing_key != Aes128Key::null() {
        conf.mesh.signing_key
    } else {
        get_signing_key(conf.mesh.root_key)
    })?;

    let plan = TxPlan {
        data_rate: conf.mesh.data_rate.clone(),
        toa: ctp::toa_for_sf(conf.mesh.data_rate.spreading_factor),
    };

    debug!("Queueing routing beacon for broadcast");

    scheduler::enqueue(packet, plan, scheduler::BEACON_FLOW_ID, None, false).await;

    Ok(())
}
