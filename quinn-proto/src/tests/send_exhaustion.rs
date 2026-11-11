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
        app_limited: bool,
        _: &crate::connection::RttEstimator,
    ) {
        let mut events = self.0.lock().unwrap();
        events.push("ack");
        if app_limited {
            events.push("legacy-limited");
        }
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
        // A randomly skipped packet number before the first write makes the server ACK that
        // write immediately, leaving no ACK for the second one.
        .deterministic_packet_numbers(true)
        .congestion_controller_factory(Arc::new(observations.clone()));
    let (client, _server) = pair.connect_with(config);
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

#[derive(Debug, Clone, Copy)]
enum CreditLimit {
    Stream,
    Connection,
    SendWindow,
}

fn credit_pair(limit: CreditLimit) -> (Pair, ConnectionHandle, ConnectionHandle, Boundaries) {
    let mut server = server_config();
    let server_transport = Arc::get_mut(&mut server.transport).unwrap();
    server_transport
        .mtu_discovery_config(None)
        .stream_receive_window(VarInt::from_u32(if matches!(limit, CreditLimit::Stream) {
            4096
        } else {
            1 << 20
        }))
        .receive_window(VarInt::from_u32(
            if matches!(limit, CreditLimit::Connection) {
                4096
            } else {
                1 << 20
            },
        ));
    let mut pair = Pair::new(Default::default(), server);
    let observations = Boundaries::default();
    let mut config = client_config();
    Arc::get_mut(&mut config.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .send_window(if matches!(limit, CreditLimit::SendWindow) {
            4096
        } else {
            1 << 20
        })
        .congestion_controller_factory(Arc::new(observations.clone()));
    let (client, server) = pair.connect_with(config);
    pair.drive();
    (pair, client, server, observations)
}

fn blocked_credit_does_not_report_source_exhaustion(limit: CreditLimit) {
    let (mut pair, client, _server, observations) = credit_pair(limit);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    assert_eq!(
        pair.client_send(client, stream).write(&[7; 4096]).unwrap(),
        4096
    );
    assert_matches!(
        pair.client_send(client, stream).write(&[7; 4096]),
        Err(WriteError::Blocked)
    );
    observations.0.lock().unwrap().clear();
    // Deliver no peer feedback: accepted bytes can leave, but the outstanding
    // application write remains blocked on this exact credit limit.
    pair.drive_client();
    let events = observations.0.lock().unwrap();
    assert!(events.contains(&"send"), "{limit:?}: {events:?}");
    assert!(pair.client_conn_mut(client).bytes_in_flight() < 64 * 1024);
    assert!(
        !events.contains(&"idle"),
        "{limit:?}: source still has a blocked write: {events:?}"
    );
    drop(events);
    pair.drive_server();
    pair.drive_client();
    assert!(
        observations.0.lock().unwrap().contains(&"legacy-limited"),
        "the generic ACK hint must retain its loss-based-controller contract"
    );
}

#[test]
fn send_exhaustion_credit_stream_blocked_writer_is_not_source_idle() {
    blocked_credit_does_not_report_source_exhaustion(CreditLimit::Stream);
}
#[test]
fn send_exhaustion_credit_connection_blocked_writer_is_not_source_idle() {
    blocked_credit_does_not_report_source_exhaustion(CreditLimit::Connection);
}
#[test]
fn send_exhaustion_credit_local_window_blocked_writer_is_not_source_idle() {
    blocked_credit_does_not_report_source_exhaustion(CreditLimit::SendWindow);
}

#[test]
fn send_exhaustion_credit_exact_limit_without_a_blocked_write_is_source_idle() {
    for limit in [
        CreditLimit::Stream,
        CreditLimit::Connection,
        CreditLimit::SendWindow,
    ] {
        let (mut pair, client, _, observations) = credit_pair(limit);
        let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
        assert_eq!(
            pair.client_send(client, stream).write(&[7; 4096]).unwrap(),
            4096
        );
        observations.0.lock().unwrap().clear();
        pair.drive_client();
        assert!(
            observations.0.lock().unwrap().contains(&"idle"),
            "{limit:?}"
        );
    }
}

#[test]
fn send_exhaustion_credit_closed_writer_does_not_hold_demand() {
    for reset in [false, true] {
        for limit in [
            CreditLimit::Stream,
            CreditLimit::Connection,
            CreditLimit::SendWindow,
        ] {
            let (mut pair, client, _, observations) = credit_pair(limit);
            let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
            assert_eq!(
                pair.client_send(client, stream).write(&[7; 4096]).unwrap(),
                4096
            );
            assert_matches!(
                pair.client_send(client, stream).write(&[7; 1]),
                Err(WriteError::Blocked)
            );
            if reset {
                pair.client_send(client, stream).reset(0u32.into()).unwrap();
            } else {
                pair.client_send(client, stream).finish().unwrap();
            }
            observations.0.lock().unwrap().clear();
            pair.drive_client();
            assert!(
                observations.0.lock().unwrap().contains(&"idle"),
                "{limit:?}, reset={reset}"
            );
        }
    }
}

#[test]
fn send_exhaustion_credit_restoration_does_not_keep_a_stale_blocker() {
    for limit in [
        CreditLimit::Stream,
        CreditLimit::Connection,
        CreditLimit::SendWindow,
    ] {
        let (mut pair, client, server, observations) = credit_pair(limit);
        let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
        assert_eq!(
            pair.client_send(client, stream).write(&[7; 4096]).unwrap(),
            4096
        );
        assert_matches!(
            pair.client_send(client, stream).write(&[7; 1]),
            Err(WriteError::Blocked)
        );
        pair.drive_client();
        pair.drive_server();
        assert_eq!(pair.server_streams(server).accept(Dir::Uni), Some(stream));
        let mut recv = pair.server_recv(server, stream);
        let mut chunks = recv.read(true).unwrap();
        let mut bytes = 0;
        while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
            assert!(chunk.bytes.iter().all(|byte| *byte == 7));
            bytes += chunk.bytes.len();
        }
        let _ = chunks.finalize();
        assert_eq!(bytes, 4096);
        observations.0.lock().unwrap().clear();
        pair.drive();
        assert!(
            observations.0.lock().unwrap().contains(&"idle"),
            "{limit:?}: stale blocked-write state"
        );
        assert_eq!(pair.client_send(client, stream).write(&[7]).unwrap(), 1);
        observations.0.lock().unwrap().clear();
        pair.drive_client();
        assert!(
            observations.0.lock().unwrap().contains(&"idle"),
            "{limit:?}: write is unblocked"
        );
    }
}

#[test]
fn send_exhaustion_credit_stopped_writer_does_not_hold_demand() {
    for limit in [
        CreditLimit::Stream,
        CreditLimit::Connection,
        CreditLimit::SendWindow,
    ] {
        let (mut pair, client, server, observations) = credit_pair(limit);
        let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
        assert_eq!(
            pair.client_send(client, stream).write(&[7; 4096]).unwrap(),
            4096
        );
        assert_matches!(
            pair.client_send(client, stream).write(&[7; 1]),
            Err(WriteError::Blocked)
        );
        pair.drive_client();
        pair.drive_server();
        assert_eq!(pair.server_streams(server).accept(Dir::Uni), Some(stream));
        pair.server_recv(server, stream).stop(0u32.into()).unwrap();
        observations.0.lock().unwrap().clear();
        pair.drive();
        assert!(
            observations.0.lock().unwrap().contains(&"idle"),
            "{limit:?}"
        );
    }
}
