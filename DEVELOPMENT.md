# Maintained EasyTier dependency

This branch extends the maintained Quinn 0.11.19 protocol checkpoint
`061829d212090b87e471d94cba890acd5cffa933`, paired with Quinn 0.11.12 and
quinn-udp 0.5.16. It retains application-limited BBR fixes and bounded byte-rate
pacing, and adds the scoped HY2 alignment described in
[HY2-RUST-UPSTREAM.md](HY2-RUST-UPSTREAM.md).

Chrome wire shaping and HY2 window/profile options are explicit. The `hy2-rustls`
feature requires EasyTier's TLS patch; builds without that feature use official
rustls. Other fork changes also apply to ordinary Quinn consumers. Adaptive loss
thresholds remain archived.

## Fork compatibility boundary

The package retains the upstream version `0.11.19` for dependency resolution.
Consumers must identify this fork by its exact Git revision. Its public Rust API
includes incompatible changes: `congestion::Controller::on_sent` and `on_ack`
accept additional in-flight state, and controller lifecycle callbacks include
congestion-event boundaries, application-limited observations, RTT updates,
losses and discarded packets. Third-party controller implementations need source
updates and explicit validation. `ConnectError::InvalidTransportParameters` also
extends a public enum, affecting exhaustive matches. The version number alone
does not promise source compatibility with crates.io Quinn.

These maintained changes are active independently of the HY2 profile switches:

- Due ACK-only feedback can bypass the congestion window and pacer, while
  remaining separate from congestion-controlled padding and GSO batches.
- Retiring Initial or Handshake keys resets PTO backoff before rearming loss
  detection. Lack of handshake progress continues to use PTO backoff.
- Stream writable-event polling rechecks current credit and stream state before
  waking blocked writers. Explicit reserve configuration changes credit policy;
  the revised polling code also runs with the default reserve values.
- The selected BBR controller uses the maintained delivery sampler, event state,
  application-limited detection and recovery/ProbeRTT fixes described below.
  Selecting another congestion controller does not enable BBR.
- Receive assembly uses a byte-funded metadata allowance and consumption-driven
  reclamation for all receive streams, including ordinary QUIC.

Profile-specific tests and ordinary QUIC tests cover different consumers. Validate
both when changing shared code; these source contracts establish no WAN
throughput or recovery guarantee.

## Upstream 0.11.x maintenance

Relative to official `0.11.x` at `8edb4028` (merge base `8192ed39`), these
`quinn-proto` fixes are incorporated, each with a regression test:

- Cherry-picked: `50f5519c` (illegal ordered reads keep receive state),
  `9ba03eb5` (nonminimal integer transport parameters), `e716bf53` (zero idle
  timeout does not refuse Initials), `98c9372f` (APPLICATION_CLOSE reason
  budget), `8edb4028` (server handshake data waits for ALPN after a
  HelloRetryRequest; without server ALPN it becomes available at completion).
- Adapted: `5303ccc1` (switching to unordered reads drops already-read
  duplicates) passes the start offset through both fork defragmentation
  helpers; metadata budgeting is unchanged. `e367976e` (DATAGRAM wire limit
  separate from memory budget): a frame within the limit that does not fit the
  receive buffer is dropped; a frame whose encoded size exceeds the limit this
  endpoint advertised is a PROTOCOL_VIOLATION. The advertised value and the
  check share one helper, so `max_datagram_frame_size` overrides apply to both.
  With `omit_max_datagram_frame_size` no limit is advertised, so received
  frames are bounded only by the receive buffer.
- Independently ported earlier: `4b52785a` (fork `06b9f575`, `c7f28f99`) and
  `77a51bdc` (fork `834b764f`, `11cc25c3`).

Deliberately not taken:

- The connection-acceptance series `8a1e53da`..`5aebe8f7` (14 commits): the
  consumer's locked `quinn` 0.11.12 wrapper never calls the new API.
- `quinn` and `quinn-udp` commits `808dcfe7`, `6e5b60d9`, `c6d94ce4`,
  `8675d095`, `c2a47f6d`, `322e38a3`, `f0f8c66f`, `4b84837d`, `b86fc003`,
  `5efe8318`: the consumer takes those crates from crates.io and uses its own
  sockets.
- Lockfile and advisory commits `b8306e19`, `974f96b3`, `49cf1206`.

`hy2_receive_history(true)` uses a fixed 16,384-packet replay window with a
2 KiB bitmap in each packet-number space. First arrivals within the window stay
eligible even after sparse/deep reordering; duplicate packet numbers and numbers
below the monotonic retirement floor remain rejected. Ordinary Quinn keeps its
compact default window. The receive-history option preserves the ACK encoder's 64-range budget and the
fork's existing loss, BBR, PTO and packet-layout policies. EasyTier
HY2 enables this receive profile explicitly. This corrects authenticated original
packet rejection; it does not establish WAN capacity or recovery acceptance.

Due ACKs remain sendable when queued application data is blocked by the congestion
window or pacer. Such feedback is emitted on its own, without optional MTU or GSO
padding, and leaves application and path-validation frames queued. Mandatory
client Initial padding and anti-amplification limits retain their existing rules.
This corrects feedback admission; it does not change PTO, congestion algorithms,
or their parameters. Product recovery and real-path throughput require separate
consumer verification.

Spurious loss detection follows upstream development commits `531ca90e`,
`459c3471` and `aa61ce99`. Packets declared lost while in flight on the current
path are recorded per packet-number space until a later loss detection finds
them sent more than two base PTOs earlier, or until that space's keys are
discarded. An ACK frame covering the last remaining record counts
`spurious_congestion_events` and calls `Controller::on_spurious_congestion_event`.
No controller in this fork implements that hook; upstream's Cubic undo is not
taken. Fork-only `PathStats` fields count each recorded packet once when its
acknowledgement arrives: `spurious_lost_packets` and `spurious_lost_bytes`
(also included in `lost_packets` / `lost_bytes`), plus the maxima
`spurious_loss_max_packet_reordering` (newer packets acknowledged before or with
it) and `spurious_loss_max_time_reordering` (how much earlier it was sent than
the newest acknowledged packet). A record holds the send time and size; records
are bounded by the in-flight packets declared lost within the drain window. An
acknowledgement arriving after its record expired is not counted, so the
counters are lower bounds. Nothing reacts to them: loss detection,
retransmission and congestion-controller inputs are unchanged.

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

PMTU probes wait until the current path is validated, following upstream 0.11
fix `77a51bdc`. The independent probe branch bypasses the ordinary send loop's
anti-amplification check; a migrated path must retain that budget for its
PATH_CHALLENGE until PATH_RESPONSE validates it. Validation timeout still
restores the previous path, and successful validation resumes normal MTU search.
The local UDP payload budget remains active before packetization and during
subsequent discovery, including budget changes while validation is pending.
This guard changes neither congestion control nor migration/PMTU defaults.

`TransportConfig::local_udp_payload_limit` is an opt-in, destination-specific
host budget queried before packetization. It clamps ordinary sends and PMTU
probes on an existing path, including when encapsulation changes at runtime.
Changing a budget cancels the old search so a late probe ACK cannot restore an
oversized MTU. Releasing it leaves growth to normal PMTU discovery. The provider
is disabled by default and changes no advertised transport parameter, congestion
algorithm, timer or ordinary socket behavior. Host and consumer integration
must be validated separately; already queued/in-flight packets retain normal
loss recovery.

`TransportConfig::max_pto_interval` is an opt-in, sender-local cap on the
backed-off probe timeout for application data once the handshake is confirmed.
The capped interval never falls below the PTO without backoff (including the
peer's maximum ACK delay); Initial and Handshake probes keep exponential
backoff, and `pto_count` still counts every expiry. Enabling it departs from
the doubling rule of RFC 9002 section 6.2.1. It changes no wire format, frame
or transport parameter and has no interoperability effect. It is off (`None`)
by default; EasyTier does not enable it.

`TransportConfig::max_unsent_stream_delay` is an opt-in, sender-local admission
policy, off (`None`) by default. When set, a write on a stream is admitted only
while that stream's unsent data (accepted but never transmitted; retransmissions
and unacknowledged bytes do not count) fits within the delay at the current
sending rate: the controller's explicit `pacing_rate`, otherwise the window
pacer's 5/4 window per smoothed RTT, never less than ten maximum-size packets.
For HY2 that rate is BBR's pacing rate, or with a negotiated Brutal rate the
configured bandwidth with loss compensation. The Brutal rate is not measured from
the path, so on a slower path the bound corresponds to a longer real wait. The
allowance is recomputed when the application takes a send-stream handle or polls
for events, and applies beside the existing credit limit. A writer it blocks joins
the existing blocked list and is woken by the event poll that follows
transmission from its own stream; no acknowledgement or credit update is needed,
and a blocked writer is not woken again until its stream has room. The
application-limited check still looks at credit only: a stream whose backlog was
fully transmitted can write again, so exhaustion at that point is reported to
the controller as the source running dry, and BBR marks the following samples
application-limited instead of treating a refill gap as a path limit.

Each stream has its own allowance, so one stream's backlog never delays admission
on another; same-priority streams still rotate per packet (`send_fairness`,
default on). The cost: n streams saturating together each hold their own
allowance and share the rate, so each sees up to n times the delay. Unset,
admission is unchanged. Tests compare digests of write admission (unset) and of
every congestion-controller call for a writer keeping up with an unconstrained
path (set and unset) against the tree without this policy. EasyTier HY2 enables
it at 100 ms.

`Controller::on_packet_discarded` retires packet bookkeeping without creating a
congestion event, delivery-rate sample or loss sample. BBR uses it when an ACK
is intentionally ignored during path validation, when Retry or rejected
0-RTT withdraws a tracked packet, and when Initial or Handshake keys retire
their packet-number space. Key retirement removes in-flight bytes while leaving
the next congestion event free of synthetic loss send-state. Controller wrappers must forward this callback
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

BBR consumes Quinn's application-limited notification at actual transmit
exhaustion. It marks subsequent packet samples through the existing packet-number
boundary; cached ACK hints do not renew that marker, and recorded ACK/loss send
states remain unchanged. An in-flight-only BDP
heuristic can mistake a light stream for a saturated path and end STARTUP at that
stream's offered rate. This is an explicit extension beyond Hysteria 2.13.0's
heuristic, with unchanged loss/no-growth thresholds. Real protocol regressions
cover light input followed by backlog and continuously backlogged input; field
queueing, convergence and fairness remain separate acceptance requirements.

Discarding Initial or Handshake keys resets PTO backoff before rearming loss
detection, following RFC 9002 sections 6.2.2 and A.11. A client that loses one or
two Initial flights still backs off while no progress is made; after sending its
first Handshake flight, loss of that flight is probed at the current base PTO.
Controlled-clock tests cover those cases and the no-loss baseline, including
actual CRYPTO retransmission timing and successful handshake completion. This
correction concerns handshake recovery; it does not establish the cause of
established-session stalls or a real-path throughput improvement.


`quic_go_packet_numbers(true)` selects the ordinary packet-number encoding used
by Hysteria 2.13.0's quic-go dependency (`a10df75c260c`): a two-byte minimum,
three bytes at an unacknowledged range of 32,768, and four bytes at 8,388,608.
With no ACK, that range is the next packet number plus one. Explicit Chrome
client shaping takes precedence; servers use the ordinary policy even if the
Chrome flag is set. The option defaults to false for ordinary Quinn consumers.
Header serialization and 1-RTT size prediction share the same selection.

The real TLS regression delays the first encrypted server datagram until over
1,213 later datagrams have arrived, then delivers the original before returning
any ACK or timer event to the sender. Under the existing short encoding the
ordered body remains incomplete; the explicit quic-go policy delivers the full
3 MiB without retransmission. This proves that encoding boundary under controlled
reordering. Interface capture order alone does not identify encrypted packet
numbers or prove this caused every observed WAN stall. EasyTier must explicitly
enable the option and validate the exact dependency revision before deployment.

The explicit BBR application-limited extension observes actual transmit
exhaustion through `Controller::on_app_limited`: no datagram was produced and
neither congestion nor pacing blocked sending. It records the last sent packet
at that boundary only when no known stream writer is still blocked on stream,
connection, or local send-window credit. Credit exhaustion alone does not imply
continued source demand: a preceding failed write is required. Closed, reset,
stopped, or recredited streams do not retain that blocker. The historical ACK-time
hint is unchanged for Reno/Cubic and does not renew the BBR sampler's marker. Merely queuing a small write does not
establish a saturated source. The BDP heuristic and historical packet send
states are unchanged; light-to-bulk and continuous-backlog regressions remain
required alongside consumer and throughput validation.

Stream assembly retains a fixed allowance of 1024 metadata nodes plus one node
per 128 buffered bytes, capped by the unread offset span before compaction and
recomputed after deduplication. Compaction starts at twice that budget; sparse
small-frame attacks remain bounded, while datagram-sized gaps in multi-MiB
receive windows can be reassembled without a connection-fatal fixed-span limit.
Consumed, cleared and reused assemblers release excess metadata capacity, with
geometric slack while unread data remains. This changes local resource accounting,
not QUIC frame layouts, flow-control credit or congestion control. Protocol and
consumer acceptance remain separate from WAN throughput and heartbeat recovery.
