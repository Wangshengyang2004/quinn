use super::*;
use crate::congestion::{Controller, ControllerFactory};

#[derive(Clone, Debug, Default)]
pub(crate) struct PacketLifecycle(pub(crate) Arc<Mutex<Vec<(&'static str, u64)>>>);

impl ControllerFactory for PacketLifecycle {
    fn build(self: Arc<Self>, _now: Instant, _mtu: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}

impl Controller for PacketLifecycle {
    fn on_sent(&mut self, _now: Instant, _bytes: u64, packet: u64, _in_flight: u64) {
        self.0.lock().unwrap().push(("sent", packet));
    }
    fn on_packet_lost(&mut self, packet: u64, _bytes: u64) {
        self.0.lock().unwrap().push(("lost", packet));
    }
    fn on_packet_discarded(&mut self, packet: u64) {
        self.0.lock().unwrap().push(("discarded", packet));
    }
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _persistent: bool,
        _lost_bytes: u64,
        _lost_packets: u64,
        _last_packet: Option<u64>,
        _in_flight: Option<u64>,
    ) {
        self.0.lock().unwrap().push(("congestion", 0));
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

pub(crate) fn recording_client(events: &PacketLifecycle) -> ClientConfig {
    let mut config = client_config();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .congestion_controller_factory(Arc::new(events.clone()));
    config
}

#[test]
fn key_discard_loss_free_handshake_reports_retirement_without_packet_loss() {
    let mut pair = Pair::default();
    let events = PacketLifecycle::default();
    let (_client, _server) = pair.connect_with(recording_client(&events));
    pair.drive();
    let events = events.0.lock().unwrap();
    assert!(events.iter().any(|(kind, _)| *kind == "sent"));
    assert!(events.iter().any(|(kind, _)| *kind == "discarded"));
    assert!(
        events
            .iter()
            .all(|(kind, _)| *kind != "lost" && *kind != "congestion"),
        "a successful loss-free handshake must not create loss samples: {events:?}"
    );
}

#[test]
fn key_retirement_pto_after_one_lost_initial() {
    check_retired_initial_pto(1);
}

#[test]
fn key_retirement_pto_after_two_lost_initials() {
    check_retired_initial_pto(2);
}

#[test]
fn key_retirement_pto_without_initial_loss() {
    check_retired_initial_pto(0);
}

fn check_retired_initial_pto(dropped_initial_flights: u32) {
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(50);
    let client = pair.begin_connect(client_config());
    for expected_backoff in 0..dropped_initial_flights {
        pair.drive_client();
        assert!(!pair.server.inbound.is_empty());
        pair.server.inbound.clear();
        let now = pair.time;
        let state = pair
            .client_conn_mut(client)
            .handshake_pto_state_for_test(now);
        assert_eq!(state.0, expected_backoff);
        assert!(state.1);
        pair.time = pair.client.next_wakeup().unwrap();
    }
    pair.drive_client();
    let now = pair.time;
    let before = pair
        .client_conn_mut(client)
        .handshake_pto_state_for_test(now);
    assert_eq!(before.0, dropped_initial_flights);
    assert!(before.1);
    pair.time += pair.latency;
    pair.drive_server();
    assert!(!pair.client.inbound.is_empty());
    pair.time += pair.latency;
    pair.drive_client();
    let now = pair.time;
    let after = pair
        .client_conn_mut(client)
        .handshake_pto_state_for_test(now);
    assert!(
        !after.1,
        "client must have retired Initial keys by sending Handshake"
    );
    assert!(after.2, "Handshake keys remain until confirmation");
    let next_pto = after.4.unwrap() - pair.time;
    eprintln!(
        "retirement dropped={dropped_initial_flights} backoff={} base={:?} next={next_pto:?}",
        after.0, after.3
    );
    assert_eq!(after.0, 0, "Initial key retirement must reset PTO backoff");
    assert_eq!(next_pto, after.3);
    // Lose the first client Finished flight, then verify the actual
    // retransmission occurs at the unbacked-off Handshake PTO.
    pair.server.inbound.clear();
    let crypto_sent = pair.client_conn_mut(client).stats().frame_tx.crypto;
    pair.time += after.3 - Duration::from_millis(1);
    pair.drive_client();
    assert_eq!(
        pair.client_conn_mut(client).stats().frame_tx.crypto,
        crypto_sent
    );
    pair.time += Duration::from_millis(1);
    pair.drive_client();
    assert!(pair.client_conn_mut(client).stats().frame_tx.crypto > crypto_sent);
    pair.drive();
    let server = pair.server.assert_accept();
    pair.finish_connect(client, server);
}
