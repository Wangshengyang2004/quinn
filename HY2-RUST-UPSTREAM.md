# HY2 protocol alignment candidate

Base: maintained `quinn-proto` 0.11.19 checkpoint
`061829d212090b87e471d94cba890acd5cffa933`, including bounded explicit-rate pacing.

The opt-in Chrome transport/Initial-packet profile, send-time congestion metadata
and BBR profile controls are rebased from `crapthings/hysteria-rust`
`d47b9a9b5ab9e8912919990cf11904878d9552b7` (its base is Quinn 0.11.16).
Hysteria 2.13.0 and quic-go `a10df75c260c` are the compatibility oracle.
The reference's license is retained in `LICENSE-HYSTERIA-RUST-PATCHES`.

Chrome shaping is opt-in. QUIC packets remain separate within each UDP datagram;
GSO batching resumes after handshake confirmation, matching quic-go. The original
reference patch also disabled GSO, which was an unnecessary runtime cost.
HY2 explicitly pins short-header spin/fixed bits to zero/one, including when a
peer advertises fixed-bit GREASE. A post-handshake bulk-wire regression checks
this independently of the Initial-flight oracle.
Receive-window auto tuning is opt-in and ported from the pinned quic-go flow
controllers under `LICENSE-QUIC-GO`: quarter-window credit updates, growth only
after consuming more than half a window within the RTT-derived interval,
doubling capped per stream/connection, and a 1.5x connection minimum when a
stream grows. Epochs start on received data; unread or slow streams do not grow.
The EasyTier adapter uses Chrome's initial 6/15 MiB and Hysteria's 8/20 MiB caps.
Ordinary connections retain minimum-MTU padding and
window pacing. HY2 configures BBR's profiles, 32-packet initial window,
20,000-packet maximum and MTU scaling; ordinary callers retain byte-window defaults.
The controller API carries send/loss metadata, a per-event prior flight size and
one fresh RTT callback per ACK frame. Controllers can explicitly choose datagram
admission at their congestion-window boundary. The default preserves Quinn's
existing check; HY2 uses quic-go's current-flight check, including Brutal's
inclusive one-MTU minimum window. Actual controller implementations and
consumers must be compiled against the new API together.

The optional `hy2-rustls` feature exposes authenticated ECH/ALPS metadata from
EasyTier's separately maintained rustls patch. Enabling it requires that explicit
TLS dependency override. Ordinary Quinn builds continue to use official rustls.

The candidate passes protocol unit tests and the ignored Go first-flight wire
comparison on the dedicated build host. The Go probe is test-only and pinned in
`tools/chrome-wire-reference`; set `HY2_CHROME_GO_PROBE` when running the ignored
test. Consumer integration, cross-platform runtime, capacity comparisons and
rollout remain separate release gates. This document does not assert deployment.
