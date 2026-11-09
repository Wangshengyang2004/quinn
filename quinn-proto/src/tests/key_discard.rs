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
        events.iter().all(|(kind, _)| *kind != "lost" && *kind != "congestion"),
        "a successful loss-free handshake must not create loss samples: {events:?}"
    );
}
