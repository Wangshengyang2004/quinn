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

Due ACKs remain sendable when queued application data is blocked by the congestion
window or pacer. Such feedback is emitted on its own, without optional MTU or GSO
padding, and leaves application and path-validation frames queued. Mandatory
client Initial padding and anti-amplification limits retain their existing rules.
This corrects feedback admission; it does not change PTO, congestion algorithms,
or their parameters. Product recovery and real-path throughput require separate
consumer verification.

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

BBR's startup decision uses the send state of the newest valid packet across
newly acknowledged and lost packets in the congestion event, matching Hysteria
2.13.0's sampler implementation. Loss-only events also retain that state. The
maximum delivery-rate sample remains a separate bandwidth input. A valid send
state is retained even when its bandwidth sample has zero duration, and callback
order cannot replace a larger packet's state. Unknown or non-congestion-controlled
packets have no sampler state and cannot overwrite it. This prevents an event
whose newest valid send state is application-limited from falsely counting a
startup bandwidth plateau; real-path throughput requires separate verification.
The history used by startup overshoot detection advances from that same completed
event state. An older full-rate ACK in an application-limited event cannot prove
overshoot; a full-rate loss-only event does contribute to the history.

EasyTier selects an exact Git revision in Cargo.toml and Cargo.lock. Updating
this branch alone does not update its consumer. Validate both protocol and
consumer paths before changing the pin or deploying. Historical experiments
remain under `archive/*` tags; restoring one does not authorize its integration.
`dev` is the integration branch; preserve `main` and its history.

An opt-in local send-credit policy reserves connection flow-control and send-buffer
space for high-priority control streams and bounded initial exchanges on new
streams. Bulk writes preserve both reserves at admission, before data enters QUIC
buffers; priority scheduling alone cannot recover consumed MAX_DATA. Reserves
are bounded by the peer's initial credit and current local send window. Blocked
writers wake when their own class has credit, avoiding a busy loop.

`low_credit_updates(true)` promptly returns consumed receive credit when at most
one quarter of the advertised receive window remains. Both mechanisms are needed
for sustained isolation: an older receiver still batches small reads, and an older
sender still admits bulk data without a reserve. These options add no wire fields,
change no advertised memory limits, and are disabled by default. EasyTier enables
them only for its explicitly negotiated native TCP extension. Ordinary QUIC and
standard HY2 keep their existing defaults. The bounded initial allowance does not
guarantee completion of arbitrary-sized new responses or defeat backpressure on
the control stream itself.

Replacement paths call `ControllerFactory::build_for_path` with the same
connection's previous controller. Its default delegates to `build`, preserving
ordinary QUIC's fresh congestion state. An application may carry authenticated
session decisions into a fresh controller; initial connections always call
`build` independently. Both true-IP migration and explicit path reset use the
hook. Same-IPv4 port rebinding keeps the existing cloned-state behavior. RTT,
pacer, MTU and congestion history retain their normal path reset contracts.

`TransportConfig::local_udp_payload_limit` is an opt-in, destination-specific
host budget queried before packetization. It clamps ordinary sends and PMTU
probes on an existing path, including when encapsulation changes at runtime.
Changing a budget cancels the old search so a late probe ACK cannot restore an
oversized MTU. Releasing it leaves growth to normal PMTU discovery. The provider
is disabled by default and changes no advertised transport parameter, congestion
algorithm, timer or ordinary socket behavior. Host and consumer integration
must be validated separately; already queued/in-flight packets retain normal
loss recovery.

`Controller::on_packet_discarded` retires packet bookkeeping without creating a
congestion event, delivery-rate sample or loss sample. BBR uses it when an ACK
is intentionally ignored during path validation, and when Retry or rejected
0-RTT withdraws a tracked packet. Controller wrappers must forward this callback
to controllers that may retain the packet, including an inactive controller
after authentication selects a different algorithm. Existing ACK/loss callbacks
retain their behavior. Same-IP port-rebinding and sampler regressions cover
record retirement and preservation of event/delivery state; consumer and field
validation remain separate requirements.
