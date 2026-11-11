//! Connection inputs to the congestion controller under deterministic reordering
//!
//! The digest printed here is compared across builds: spurious-loss bookkeeping must not change
//! loss detection, retransmission or congestion-controller calls.
use super::*;
use crate::congestion::{Controller, ControllerFactory};
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug, Default)]
pub(super) struct Calls(pub(super) Arc<Mutex<Vec<(&'static str, u64, u64)>>>);

impl Calls {
    fn push(&self, call: &'static str, a: u64, b: u64) {
        self.0.lock().unwrap().push((call, a, b));
    }
}

impl ControllerFactory for Calls {
    fn build(self: Arc<Self>, _now: Instant, _mtu: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}

impl Controller for Calls {
    fn on_sent(&mut self, _now: Instant, bytes: u64, packet: u64, _in_flight: u64) {
        self.push("sent", packet, bytes);
    }
    fn on_begin_congestion_event(&mut self, prior_bytes_in_flight: u64) {
        self.push("begin", prior_bytes_in_flight, 0);
    }
    fn on_packet_lost(&mut self, packet: u64, bytes: u64) {
        self.push("lost", packet, bytes);
    }
    fn on_packet_discarded(&mut self, packet: u64) {
        self.push("discarded", packet, 0);
    }
    fn on_ack(
        &mut self,
        _now: Instant,
        _sent: Instant,
        packet: u64,
        bytes: u64,
        _in_flight_at_send: u64,
        _app_limited: bool,
        _rtt: &crate::connection::RttEstimator,
    ) {
        self.push("ack", packet, bytes);
    }
    fn on_end_acks(
        &mut self,
        _now: Instant,
        in_flight: u64,
        _app_limited: bool,
        largest_acked: Option<u64>,
    ) {
        self.push("end", in_flight, largest_acked.unwrap_or(u64::MAX));
    }
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _persistent: bool,
        lost_bytes: u64,
        lost_packets: u64,
        _last_packet: Option<u64>,
        _in_flight: Option<u64>,
    ) {
        self.push("congestion", lost_bytes, lost_packets);
    }
    fn on_mtu_update(&mut self, _mtu: u16) {}
    fn window(&self) -> u64 {
        64 * 1024
    }
    fn initial_window(&self) -> u64 {
        self.window()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

/// An established connection with deterministic packet numbers, whose client reports every
/// congestion-controller call to `calls`
pub(super) fn reordering_pair(calls: &Calls) -> (Pair, ConnectionHandle, ConnectionHandle) {
    let mut pair = Pair::default_with_deterministic_pns();
    let mut config = client_config_with_deterministic_pns();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .mtu_discovery_config(None)
        // Only the packet threshold declares the held packets lost.
        .time_threshold(1000.0)
        .congestion_controller_factory(Arc::new(calls.clone()));
    let (client_ch, server_ch) = pair.connect_with(config);
    pair.drive();
    (pair, client_ch, server_ch)
}

/// Write `rounds` times. Each round holds back the client's first datagram until the server has
/// acknowledged the rest and the client has processed that ACK, so the packet threshold deems the
/// held packet lost. With `deliver_late` the held datagram is delivered afterwards, otherwise it
/// is lost.
pub(super) fn reordering_rounds(
    pair: &mut Pair,
    client_ch: ConnectionHandle,
    rounds: usize,
    deliver_late: bool,
) {
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    for _ in 0..rounds {
        pair.client_send(client_ch, s)
            .write(&[0xab; 8 * 1200])
            .unwrap();
        pair.drive_client();
        let held = pair.server.inbound.pop_front().unwrap();
        assert!(pair.server.inbound.len() >= 3, "too few later packets");
        pair.drive_server();
        pair.drive_client();
        if deliver_late {
            pair.server.inbound.push_back(held);
        }
        pair.drive();
    }
}

#[test]
fn reordering_controller_calls_are_deterministic() {
    let _guard = subscribe();
    let run = || {
        let calls = Calls::default();
        let (mut pair, client_ch, _) = reordering_pair(&calls);
        calls.0.lock().unwrap().clear();
        reordering_rounds(&mut pair, client_ch, 20, true);
        let stats = pair.client_conn_mut(client_ch).stats();
        let calls = calls.0.lock().unwrap().clone();
        (
            calls,
            stats.path.lost_packets,
            stats.path.lost_bytes,
            stats.path.congestion_events,
            stats.path.sent_packets,
            stats.frame_tx.stream,
        )
    };
    let first = run();
    assert!(first.1 > 0, "the reordering must cause losses");
    assert_eq!(first, run());
    let mut hasher = DefaultHasher::new();
    first.hash(&mut hasher);
    println!(
        "REORDERING_CALLS calls={} lost_packets={} lost_bytes={} congestion_events={} sent_packets={} stream_frames={} digest={:016x}",
        first.0.len(),
        first.1,
        first.2,
        first.3,
        first.4,
        first.5,
        hasher.finish()
    );
}
