// Function bodies in this module are regularly cfg'd out
#![allow(unused_variables)]

#[cfg(feature = "qlog")]
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(feature = "qlog")]
use qlog::{
    events::{
        Event, EventData,
        quic::{
            PacketHeader, PacketLost, PacketLostTrigger, PacketReceived, PacketSent, PacketType,
        },
    },
    streamer::QlogStreamer,
};
#[cfg(feature = "qlog")]
use tracing::warn;

use crate::{
    ConnectionId, Instant,
    connection::{PathData, SentPacket},
    packet::SpaceId,
};

/// Shareable handle to a single qlog output stream
#[cfg(feature = "qlog")]
#[derive(Clone)]
pub struct QlogStream(pub(crate) Arc<Mutex<QlogStreamer>>);

#[cfg(feature = "qlog")]
impl QlogStream {
    fn emit_event(&self, orig_rem_cid: ConnectionId, event: EventData, now: Instant) {
        // Time will be overwritten by `add_event_with_instant`
        let mut event = Event::with_time(0.0, event);
        event.group_id = Some(orig_rem_cid.to_string());

        let mut qlog_streamer = self.0.lock().unwrap();
        if let Err(e) = qlog_streamer.add_event_with_instant(event, now) {
            warn!("could not emit qlog event: {e}");
        }
    }
}

/// A [`QlogStream`] that may be either dynamically disabled or compiled out entirely
#[derive(Clone, Default)]
pub(crate) struct QlogSink {
    #[cfg(feature = "qlog")]
    stream: Option<QlogStream>,
}

impl QlogSink {
    pub(crate) fn is_enabled(&self) -> bool {
        #[cfg(feature = "qlog")]
        {
            self.stream.is_some()
        }
        #[cfg(not(feature = "qlog"))]
        {
            false
        }
    }

    pub(super) fn emit_recovery_metrics(
        &self,
        pto_count: u32,
        path: &mut PathData,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        #[cfg(feature = "qlog")]
        {
            let Some(stream) = self.stream.as_ref() else {
                return;
            };

            let Some(metrics) = path.qlog_recovery_metrics(pto_count) else {
                return;
            };

            stream.emit_event(orig_rem_cid, EventData::MetricsUpdated(metrics), now);
        }
    }

    pub(super) fn emit_packet_lost(
        &self,
        pn: u64,
        info: &SentPacket,
        loss_delay: Duration,
        space: SpaceId,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        #[cfg(feature = "qlog")]
        {
            let Some(stream) = self.stream.as_ref() else {
                return;
            };

            let event = PacketLost {
                header: Some(PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, false),
                    length: Some(info.size),
                    ..Default::default()
                }),
                frames: None,
                trigger: Some(
                    match now.saturating_duration_since(info.time_sent) >= loss_delay {
                        true => PacketLostTrigger::TimeThreshold,
                        false => PacketLostTrigger::ReorderingThreshold,
                    },
                ),
            };

            stream.emit_event(orig_rem_cid, EventData::PacketLost(event), now);
        }
    }

    pub(super) fn emit_packet_sent(
        &self,
        pn: u64,
        len: usize,
        space: SpaceId,
        is_0rtt: bool,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        #[cfg(feature = "qlog")]
        {
            let Some(stream) = self.stream.as_ref() else {
                return;
            };

            let event = PacketSent {
                header: PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, is_0rtt),
                    length: Some(len as u16),
                    ..Default::default()
                },
                ..Default::default()
            };

            stream.emit_event(orig_rem_cid, EventData::PacketSent(event), now);
        }
    }

    pub(super) fn emit_packet_received(
        &self,
        pn: u64,
        space: SpaceId,
        is_0rtt: bool,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        #[cfg(feature = "qlog")]
        {
            let Some(stream) = self.stream.as_ref() else {
                return;
            };

            let event = PacketReceived {
                header: PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, is_0rtt),
                    ..Default::default()
                },
                ..Default::default()
            };

            stream.emit_event(orig_rem_cid, EventData::PacketReceived(event), now);
        }
    }
}

#[cfg(feature = "qlog")]
impl From<Option<QlogStream>> for QlogSink {
    fn from(stream: Option<QlogStream>) -> Self {
        Self { stream }
    }
}

#[cfg(feature = "qlog")]
fn packet_type(space: SpaceId, is_0rtt: bool) -> PacketType {
    match space {
        SpaceId::Initial => PacketType::Initial,
        SpaceId::Handshake => PacketType::Handshake,
        SpaceId::Data if is_0rtt => PacketType::ZeroRtt,
        SpaceId::Data => PacketType::OneRtt,
    }
}

#[cfg(all(test, feature = "qlog"))]
mod tests {
    use std::io::{self, Write};

    use super::*;
    use crate::QlogConfig;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn packet_loss_trigger_matches_elapsed_send_time() {
        let start = Instant::now();
        let now = start + Duration::from_secs(1);
        let loss_delay = Duration::from_millis(100);
        let cases = [
            (now, "reordering_threshold"),
            (now - Duration::from_millis(99), "reordering_threshold"),
            (now - loss_delay, "time_threshold"),
            (now - Duration::from_millis(101), "time_threshold"),
            (now + Duration::from_millis(101), "reordering_threshold"),
        ];
        let mut observed = Vec::new();
        let mut expected = Vec::new();
        for (space, packet_type) in [
            (SpaceId::Initial, "initial"),
            (SpaceId::Handshake, "handshake"),
            (SpaceId::Data, "1RTT"),
        ] {
            for (time_sent, trigger) in cases {
                let capture = Capture::default();
                let mut config = QlogConfig::default();
                config.writer(Box::new(capture.clone())).start_time(start);
                let stream = config.into_stream().unwrap();
                let sink = QlogSink::from(Some(stream));
                let packet = SentPacket {
                    congestion_sequence: 0,
                    path_generation: 0,
                    time_sent,
                    size: 1234,
                    bytes_in_flight_at_send: 1234,
                    ack_eliciting: true,
                    largest_acked: None,
                    retransmits: Default::default(),
                    stream_frames: Default::default(),
                };
                sink.emit_packet_lost(42, &packet, loss_delay, space, now, ConnectionId::new(&[1]));
                drop(sink);
                let bytes = capture.0.lock().unwrap();
                let records: Vec<serde_json::Value> = bytes
                    .split(|byte| *byte == 0x1e)
                    .filter(|record| !record.is_empty())
                    .map(|record| serde_json::from_slice(record).unwrap())
                    .collect();
                let events: Vec<_> = records
                    .iter()
                    .filter(|record| record["name"] == "recovery:packet_lost")
                    .collect();
                assert_eq!(events.len(), 1, "unexpected qlog records: {records:?}");
                let event = events[0];
                assert_eq!(event["data"]["header"]["packet_number"], 42);
                assert_eq!(event["data"]["header"]["length"], 1234);
                assert_eq!(event["data"]["header"]["packet_type"], packet_type);
                assert_eq!(event["group_id"], "01");
                observed.push(event["data"]["trigger"].as_str().unwrap().to_owned());
                expected.push(trigger);
            }
        }
        assert_eq!(observed, expected);
    }
}
