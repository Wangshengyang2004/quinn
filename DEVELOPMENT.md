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
newly acknowledged and lost packets in the congestion event. Hysteria 2.13.0
also selects the startup loss denominator from sampler state. Loss-only events
retain that state; the maximum delivery-rate sample remains a separate input.
Unknown or non-congestion-controlled packets have no sampler state and cannot
overwrite it. The sampler stores both the application-limited flag and in-flight
bytes (including the sent packet) at transmission. STARTUP loss exit uses that
same valid state, including when the aggregate loss callback names a newer packet
absent from the sampler. Callback order cannot replace a larger packet's state.

One retained Rust boundary differs from Hysteria 2.13.0: a known ACK contributes
its send state even when no bandwidth sample can be formed, including a zero ACK
time interval. Go omits the ACK state in those cases. This is covered by the Rust
sampler contract and an explicit Go boundary test; it must not be described as
exact behavior equivalence. Field impact requires separate verification.

The history used by startup overshoot detection advances from that same completed
event state. An older full-rate ACK in an application-limited event cannot prove
overshoot; a full-rate loss-only event does contribute to the history. These state
contracts and the loss-denominator correction do not establish a throughput gain.

EasyTier selects an exact Git revision in Cargo.toml and Cargo.lock. Updating
this branch alone does not update its consumer. Validate both protocol and
consumer paths before changing the pin or deploying. Historical experiments
remain under `archive/*` tags; restoring one does not authorize its integration.
`dev` is the integration branch; preserve `main` and its history.

Stopping a receive stream after RESET_STREAM returns no additional connection
receive credit: the reset already released credit through the final offset.
This follows upstream 0.11 fix `4b52785a` and preserves ordinary stop, stream
window autotuning and the opt-in low-credit update policy. The regression covers
unread received data followed by reset and stop; HY2 stream-drop integration
requires separate consumer verification.

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

Disabling `ack_frequency_supported` omits the local `min_ack_delay` transport
parameter and rejects received ACK_FREQUENCY and IMMEDIATE_ACK frames before
they can change ACK state. Rejection is a PROTOCOL_VIOLATION carrying the frame
type. Capability is directional: an endpoint may request ACK behavior from a
peer that advertises support while omitting its own receive capability.
The default remains enabled. This admission check changes neither PTO backoff
nor congestion parameters; throughput and recovery need consumer verification.

Delivery-rate filtering selects the maximum valid ACK sample of the completed
congestion event together with that sample's application-limited flag, then
updates the bandwidth filter once. A smaller full-rate sample in the same batch
cannot first expire the old peak and thereby admit an application-limited event
maximum against an already-reduced estimate. The maximum sample and the newest
packet send state used for STARTUP remain distinct inputs. Deterministic mixed
batch and filter-expiry cases are compared with Hysteria 2.13.0; this alignment
does not establish a field throughput benefit or full event-path equivalence.

BBR also consumes Quinn's application-limited hint after each ACK batch. It marks
subsequent packet samples as application-limited through the existing packet-number
boundary; recorded ACK/loss send states remain unchanged. An in-flight-only BDP
heuristic can mistake a light stream for a saturated path and end STARTUP at that
stream's offered rate. This is an explicit extension beyond Hysteria 2.13.0's
heuristic, with unchanged loss/no-growth thresholds. Real protocol regressions
cover light input followed by backlog and continuously backlogged input; field
queueing, convergence and fairness remain separate acceptance requirements.
