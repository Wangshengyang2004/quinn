# Maintained EasyTier dependency

This fork maintains `quinn-proto` 0.11.19, Quinn 0.11.12 and quinn-udp 0.5.16.
The proto extends checkpoint `061829d212090b87e471d94cba890acd5cffa933`,
retaining bounded explicit-rate pacing.
[HY2-RUST-UPSTREAM.md](HY2-RUST-UPSTREAM.md) describes
Chrome wire shaping, receive-window tuning, HY2 BBR profiles and the TLS boundary.
The optional `hy2-rustls` feature requires EasyTier's rustls patch; other builds
use official rustls.

## Integration and compatibility

The package keeps upstream version `0.11.19`, but consumers must identify the
fork by its exact Git revision. EasyTier pins that revision in Cargo.toml and
Cargo.lock; changing this branch alone does not update EasyTier. `dev` is the
integration branch; preserve `main` and its history. Historical experiments remain
under `archive/*` tags and require separate review before integration.

The public Rust API differs from crates.io Quinn: `Controller::on_sent` and
`on_ack` accept additional in-flight state, and controller callbacks include
congestion-event boundaries, application-limited observations, RTT updates,
losses and discarded packets. Third-party controllers need source updates and
validation. `ConnectError::InvalidTransportParameters` extends a public enum and
affects exhaustive matches.

Shared changes below apply to ordinary QUIC as well as HY2 unless explicitly
opt-in. Changing them requires affected protocol and consumer tests. Controlled
models establish the tested behavior; WAN throughput, recovery and fairness
require separate field acceptance. Build recipes, execution results and rollout
evidence belong in private ops.

## Upstream maintenance and packet handling

The fork merges upstream `0.11.x` through `a9ae122f`, including the three-phase
`Endpoint::start_accept`/`Accepting::accept`/`Endpoint::finish_accept` path that the
wrapper uses to run TLS setup outside the endpoint lock; `Accepting` and `Accepted`
carry the first datagram's UDP framing. UDP envelope metadata crosses quinn-udp, the wrapper and proto;
consumers using it must select all three fork crates at the same revision.
`Endpoint::handle` remains the ordinary metadata-free entry point; the wrapper
uses `handle_with_framing`. Fresh authenticated 1-RTT packets confirm framing
only on validated paths. On a candidate path, fresh AEAD- and dedup-accepted
1-RTT evidence may select its outgoing envelope and payload budget; the matching
PATH_RESPONSE confirms the path. Initial replies retain a separate tentative
hint. Local socket rebinding clears envelope evidence on current and retained
paths and queued path responses. Framing is local metadata and does not alter
QUIC packets.

DATAGRAM wire size and receive memory have separate limits. A valid-size frame
that exceeds the receive buffer is dropped; a frame larger than the advertised
`max_datagram_frame_size` is a PROTOCOL_VIOLATION. Advertising and checking use
the same limit, including overrides. `omit_max_datagram_frame_size` advertises no
limit and bounds received frames only by the receive buffer.

Due ACK-only feedback bypasses the congestion window and pacer in its own packet,
leaving application and path-validation frames queued. It receives no optional MTU
or GSO padding; mandatory client Initial padding and anti-amplification still apply.

Discarding Initial or Handshake keys resets PTO backoff before rearming loss
detection (RFC 9002 sections 6.2.2 and A.11). Flights without handshake progress
continue backing off. PMTU probes wait for path validation so PATH_CHALLENGE
retains the anti-amplification budget. Validation timeout restores the previous
path; successful validation resumes MTU search.

`quic_go_packet_numbers(true)` selects Hysteria 2.13.0/quic-go `a10df75c260c`'s
encoding: a two-byte minimum, three bytes at an unacknowledged range of 32,768,
and four at 8,388,608. Without ACKs the range is the next packet number plus one.
Chrome client shaping takes precedence; servers use the ordinary policy even
with the Chrome flag. Header serialization and 1-RTT size prediction share this
selection. The option defaults to false and requires explicit consumer selection.

Disabling `ack_frequency_supported` omits local `min_ack_delay` and rejects
ACK_FREQUENCY and IMMEDIATE_ACK with a frame-typed PROTOCOL_VIOLATION before
changing ACK state. Support defaults to enabled and is directional: an endpoint
can request ACK behavior from a capable peer while omitting its own capability.

## Receive memory and send admission

Receive assembly allows 1024 metadata nodes plus one per 128 buffered bytes,
capped by the unread offset span before compaction and recomputed after
deduplication. Compaction starts at twice the allowance. Consumed, cleared and
reused assemblers release excess metadata capacity, with geometric slack while
unread data remains. This resource accounting preserves QUIC layouts and credit.
Stopping a stream after RESET_STREAM returns no additional connection credit;
the reset already released credit through its final offset.

`hy2_receive_history(true)` uses a fixed 16,384-packet replay window and 2 KiB
bitmap per packet-number space. First arrivals inside it remain eligible under
sparse reordering; duplicates and numbers below the monotonic floor are rejected.
The 64-range ACK budget and loss policies remain unchanged. EasyTier HY2 enables
it; ordinary Quinn retains its compact default window.

The opt-in send-credit reserve protects connection credit and local send-buffer
space for high-priority control streams and bounded initial exchanges. Bulk
writes preserve both reserves at admission; reserves are bounded by peer initial
credit and the local send window. Writable-event polling rechecks each writer's
current credit and state before waking it, including with default reserves.
`low_credit_updates(true)` returns consumed credit when at most one quarter of
the advertised receive window remains. Both options default off and add no wire
fields or advertised memory limits. EasyTier enables them only for its negotiated
native TCP extension. Sustained isolation needs both peers' policies; arbitrary
new responses and control-stream backpressure remain bounded by available credit.

`TransportConfig::max_unsent_stream_delay` defaults to `None`; EasyTier HY2 uses
100 ms. It bounds each stream's accepted-but-unsent data at the current sending
rate, excluding retransmissions and unacknowledged bytes. The rate is the
controller's explicit pacing rate, otherwise 5/4 of its window per smoothed RTT,
with a minimum allowance of ten maximum-size packets. HY2 uses BBR's pacing rate
or negotiated Brutal bandwidth with loss compensation. A configured rate above
path capacity makes the real delay longer.

The allowance is recomputed when taking a send handle or polling events, beside
the existing credit limit. Transmission from that stream wakes its blocked writer
once room is available, without waiting for an ACK. Each stream has its own
allowance; n saturated streams share the rate and can each see n times the delay.
Same-priority streams retain per-packet rotation (`send_fairness`, default on).
Application-limited detection still checks credit: draining transmitted backlog
and leaving a refill gap marks subsequent samples application-limited.

## Path and timer policies

Replacement paths call `ControllerFactory::build_for_path` with the previous
controller from the same connection. Its default calls `build` for fresh state;
applications may carry authenticated session decisions into the new controller.
Initial connections independently call `build`. True-IP migration and explicit
reset use the hook; same-IPv4 port rebinding retains cloned state. RTT, pacer,
MTU and congestion history keep their normal reset contracts.

`TransportConfig::local_udp_payload_limit` is an opt-in destination-specific host
budget, disabled by default. It clamps ordinary sends and PMTU probes before
packetization, including runtime encapsulation changes. A budget change cancels
the old search so late probe ACKs cannot restore an oversized MTU; releasing it
allows normal discovery. Already queued/in-flight packets retain loss recovery.
It changes no advertised transport parameter, congestion algorithm or timer.

`TransportConfig::max_pto_interval` is an opt-in, sender-local cap on the
backed-off probe timeout for application data once the handshake is confirmed.
The capped interval never falls below the PTO without backoff (including the
peer's maximum ACK delay); Initial and Handshake probes keep exponential
backoff, and `pto_count` still counts every expiry. Enabling it departs from
the doubling rule of RFC 9002 section 6.2.1. It changes no wire format, frame
or transport parameter and has no interoperability effect. It is off (`None`)
by default. EasyTier leaves it unset unless explicitly configured through its
HY2 URL parameter. The existing recovery fixture fills the congestion window,
retains queued data, and checks complete byte delivery and FIN after a five-second
blackhole, with and without NAT rebinding.

## Loss detection and diagnostics

Spurious-loss tracking follows upstream `531ca90e`, `459c3471` and `aa61ce99`.
Current-path packets declared lost remain recorded per packet-number space until
a later loss detection finds them older than two base PTOs, or the keys retire.
An ACK covering the last record increments `spurious_congestion_events` and calls
`Controller::on_spurious_congestion_event`; no fork controller implements that
hook, and upstream Cubic undo is omitted.

Fork `PathStats` counts each recorded packet's late ACK once in
`spurious_lost_packets`/`spurious_lost_bytes` (also included in lost totals).
`spurious_loss_max_packet_reordering` measures newer packets acknowledged before
or with it; `spurious_loss_max_time_reordering` measures send-time distance from
the newest acknowledged packet. Records contain send time and size, bounded by
in-flight losses within the drain window. Expired records make these lower bounds.

`adaptive_loss_thresholds(true)` is sender-local and defaults off. After a
recorded spurious loss, the path disables packet-threshold loss detection and
uses the larger of `time_threshold` RTTs and one RTT plus a reordering window;
RTT here is max(latest, smoothed). The window is the largest observed spurious
send-time distance plus one quarter, capped at the smoothed RTT. Each observation
renews it for 16 recovery episodes, then defaults resume. An episode begins when
a congestion event's newest lost packet was sent after the previous episode began.
Path reset and address migration clear learning; same-IPv4 port rebinding keeps
it. `PathStats::reordering_window` is `None` under default thresholds.

This can delay real loss detection by up to one smoothed RTT beyond the time
threshold, with no undo of earlier congestion responses. Learning covers only
retained records and send-time distance underestimates sparse-sender lateness.
The option changes no wire format; disabled, it retains no adaptive state and
preserves loss decisions and controller inputs.

Qlog loss events classify the time threshold from elapsed transmission time.
TRACE packet events carry connection grouping, side and packet-number space;
send/authenticated-receive events include packet numbers, ACK events ranges and
delay, and loss events congestion-accounted size and timing. The grouping uses
the original Initial destination CID: stable locally, potentially different
across Retry, and independent of application peer IDs. No payload or keys are
added. Capture is host-logger opt-in and can be expensive.

## BBR event and application-limited behavior

Selecting BBR uses the maintained delivery sampler, recovery and ProbeRTT logic;
other controllers retain their own behavior. Recovery advances only on an event
acknowledging a new congestion-controlled packet, matching Hysteria 2.13.0.
Loss-only events update an existing recovery window and lost-byte accounting.

STARTUP uses send state from the newest valid acknowledged/lost packet, including
its application-limited flag and in-flight bytes at transmission. Loss-only events
retain it; unknown or non-congestion-controlled packets cannot overwrite it.
Overshoot history advances from that completed event: older full-rate ACKs cannot
turn an application-limited event into evidence of overshoot, while a full-rate
loss-only event contributes. The maximum delivery-rate sample is a separate input:
its own application-limited flag updates the bandwidth filter once per event,
avoiding smaller samples expiring the previous peak first.

One retained Rust boundary differs from Hysteria: a known ACK contributes send
state even without a bandwidth sample, including a zero ACK-time interval. The
fork therefore claims scoped alignment, with this explicit difference.

`Controller::on_packet_discarded` retires tracked packets without a congestion
event, delivery-rate sample or loss sample. BBR uses it for ignored ACKs during
path validation, Retry/rejected 0-RTT and packet-space key retirement. Wrappers
must forward it even to an inactive controller that may retain the packet.

`Controller::on_app_limited` reports actual transmit exhaustion: no datagram was
produced and congestion/pacing did not block sending. BBR marks through the last
sent packet only when no stream writer remains blocked on stream, connection or
local send-window credit. A preceding failed write establishes demand; closed,
reset, stopped or recredited streams release that blocker. ACK-time hints remain
unchanged for Reno/Cubic and do not renew BBR's marker. Recorded send states stay
fixed; ProbeRTT continues marking its traffic application-limited.

BBR lets STARTUP's excessive-loss criterion override the in-flight BDP heuristic:
at least eight loss events in a round and lost bytes above 2% of in-flight bytes
recorded at send. An application-limited round contributes no bandwidth plateau,
but meeting this criterion exits STARTUP and suppresses the heuristic until a
later round completes without meeting it. Explicit transmit-exhaustion and
ProbeRTT marking continue; a marker ends at the first ACK beyond it. This differs
from Hysteria reference `3b64f669`, which skips loss exit on application-limited
rounds and always applies the heuristic. Random loss can also meet the criterion;
a short flight blocked on credit loses heuristic protection while it holds.
