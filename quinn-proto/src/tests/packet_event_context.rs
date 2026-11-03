//! The product logger drops spans; packet diagnostics must be usable as events.
use super::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::Interest,
};

#[derive(Clone, Default)]
struct Events(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
struct Fields(BTreeMap<String, String>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl Subscriber for Events {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event()
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.is_event() {
            Interest::always()
        } else {
            Interest::never()
        }
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::TRACE)
    }
}
#[test]
fn packet_context_survives_event_only_subscriber() {
    let events = Events::default();
    tracing::subscriber::with_default(events.clone(), || {
        let mut pair = Pair::default();
        let ch = pair.begin_connect(client_config());
        pair.client.drive(pair.time, pair.server.addr);
        assert!(!pair.client.outbound.is_empty());
        pair.client.outbound.clear();
        pair.drive();
        assert_matches!(
            pair.client_conn_mut(ch).poll(),
            Some(crate::Event::HandshakeDataReady)
        );
        assert_matches!(
            pair.client_conn_mut(ch).poll(),
            Some(crate::Event::Connected)
        );
    });
    let rows = events.0.lock().unwrap();
    for (message, required) in [
        (
            "packet sent",
            vec![
                "connection",
                "side",
                "space",
                "packet_number",
                "packet_length",
            ],
        ),
        (
            "packet received",
            vec!["connection", "side", "space", "packet_number"],
        ),
        ("ack received", vec!["connection", "side", "space", "ack"]),
        (
            "packets lost",
            vec!["connection", "side", "space", "lost_packets", "lost_bytes"],
        ),
        (
            "packet declared lost",
            vec![
                "connection",
                "side",
                "space",
                "packet_number",
                "congestion_bytes",
                "elapsed_us",
                "loss_delay_us",
                "time_threshold",
            ],
        ),
    ] {
        let selected: Vec<_> = rows
            .iter()
            .filter(|r| r.get("message").is_some_and(|s| s == message))
            .collect();
        assert!(
            !selected.is_empty(),
            "missing event-only packet diagnostic: {message}"
        );
        for row in selected {
            for field in &required {
                assert!(
                    row.contains_key(*field),
                    "{message} missing {field}: {row:?}"
                );
            }
        }
    }
    for lost in rows.iter().filter(|r| {
        r.get("message")
            .is_some_and(|s| s == "packet declared lost")
    }) {
        assert!(rows.iter().any(|sent| {
            sent.get("message").is_some_and(|s| s == "packet sent")
                && ["connection", "side", "space", "packet_number"]
                    .iter()
                    .all(|key| sent.get(*key) == lost.get(*key))
        }));
        let elapsed: u128 = lost["elapsed_us"].parse().unwrap();
        let delay: u128 = lost["loss_delay_us"].parse().unwrap();
        assert_eq!(lost["time_threshold"], (elapsed >= delay).to_string());
    }
    assert!(rows.iter().any(|r| {
        r.get("ack")
            .is_some_and(|s| s.contains("ranges: \"[") && s.contains("delay:"))
    }));
}
