use super::*;
use crate::congestion::{Controller, ControllerFactory};

#[derive(Debug, Clone, Default)]
struct Boundaries(Arc<Mutex<Vec<&'static str>>>);

impl ControllerFactory for Boundaries {
    fn build(self: Arc<Self>, _now: Instant, _mtu: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}

impl Controller for Boundaries {
    fn on_sent(&mut self, _: Instant, _: u64, _: u64, _: u64) {
        self.0.lock().unwrap().push("send");
    }
    fn on_app_limited(&mut self) {
        self.0.lock().unwrap().push("idle");
    }
    fn on_ack(
        &mut self,
        _: Instant,
        _: Instant,
        _: u64,
        _: u64,
        _: u64,
        _: bool,
        _: &crate::connection::RttEstimator,
    ) {
        self.0.lock().unwrap().push("ack");
    }
    fn on_congestion_event(
        &mut self,
        _: Instant,
        _: Instant,
        _: bool,
        _: u64,
        _: u64,
        _: Option<u64>,
        _: Option<u64>,
    ) {
    }
    fn on_mtu_update(&mut self, _: u16) {}
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

#[test]
fn send_exhaustion_queued_stream_is_not_repeated_by_an_old_ack() {
    check_boundary(false, false);
}

#[test]
fn send_exhaustion_queued_datagrams_are_not_repeated_by_an_old_ack() {
    check_boundary(true, false);
}

#[test]
fn send_exhaustion_light_write_still_reports_actual_exhaustion() {
    check_boundary(false, true);
}

fn check_boundary(datagrams: bool, light: bool) {
    let mut pair = Pair::default();
    let observations = Boundaries::default();
    let mut config = client_config();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .congestion_controller_factory(Arc::new(observations.clone()));
    let (client, server) = pair.connect_with(config);
    pair.drive();
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    for byte in [1, 2] {
        assert_eq!(pair.client_send(client, stream).write(&[byte]).unwrap(), 1);
        pair.drive_client();
        pair.drive_server();
    }
    assert!(
        !pair.client.inbound.is_empty(),
        "fixture needs an encrypted ACK"
    );
    if datagrams {
        for _ in 0..100 {
            pair.client_datagrams(client)
                .send(Bytes::from(vec![7; 1024]), false)
                .unwrap();
        }
    } else {
        let bytes = if light { 368 } else { 128 * 1024 };
        assert_eq!(
            pair.client_send(client, stream)
                .write(&vec![7; bytes])
                .unwrap(),
            bytes
        );
    }
    observations.0.lock().unwrap().clear();
    pair.drive_client();
    let events = observations.0.lock().unwrap();
    assert!(events.contains(&"ack"), "{events:?}");
    assert!(events.contains(&"send"), "{events:?}");
    assert_eq!(events.contains(&"idle"), light, "{events:?}");
    if light {
        assert_eq!(events.last(), Some(&"idle"), "exhaustion follows sending");
    }
}
