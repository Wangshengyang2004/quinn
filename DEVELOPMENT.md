# Maintained EasyTier dependency

This branch extends the maintained Quinn 0.11.19 protocol checkpoint
`061829d212090b87e471d94cba890acd5cffa933`, paired with Quinn 0.11.12 and
quinn-udp 0.5.16. It retains application-limited BBR fixes and bounded byte-rate
pacing, and adds the scoped HY2 alignment described in
[HY2-RUST-UPSTREAM.md](HY2-RUST-UPSTREAM.md).

Chrome wire shaping and HY2 window/profile options are explicit. Ordinary Quinn
builds and default settings retain their existing contracts. The `hy2-rustls`
feature requires EasyTier's TLS patch; default builds use official rustls.
Adaptive loss thresholds and extended receive-history experiments remain archived.

With qlog enabled, packet-loss events classify the time threshold using the
elapsed time since transmission, matching the existing loss detector. The
serialized-event regression covers the threshold boundary and all three packet
number spaces. This corrects diagnostic labels; loss detection, congestion
control, and retransmission behavior are unchanged.

Packet diagnostics at TRACE carry their connection grouping ID and packet-number
space as event fields. Sent / authenticated received packets include the packet
number, ACK events retain ranges and delay, and declared-loss events include the
congestion-accounted size and elapsed / threshold timing. This remains usable by
an event-only subscriber that drops tracing spans. Capture is opt-in through the
host logger; enabled TRACE can be expensive and is not a capacity benchmark.
The connection field uses the original Initial destination CID, which remains
stable within the local connection; the side field separates both directions.
These are protocol grouping fields, with no application peer-ID guarantee.
Retry can change the Initial CID observed by the accepting endpoint. No payload or keys are
included in these additional events.

BBR advances recovery state and its recovery boundary only when an event
acknowledges a new congestion-controlled packet, matching Hysteria 2.13.0's
`OnCongestionEventEx`. Loss-only events still update an existing recovery window
and account for lost bytes. Regressions cover a loss-only event outside recovery,
a loss-only event during growth, and an ACK/loss batch that enters recovery.
This behavior alignment does not establish a throughput improvement.

EasyTier selects an exact Git revision in Cargo.toml and Cargo.lock. Updating
this branch alone does not update its consumer. Validate both protocol and
consumer paths before changing the pin or deploying. Historical experiments
remain under `archive/*` tags; restoring one does not authorize its integration.
`dev` is the integration branch; preserve `main` and its history.
