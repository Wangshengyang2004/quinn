# HY2 protocol alignment candidate

Base: maintained `quinn-proto` 0.11.19 checkpoint
`061829d212090b87e471d94cba890acd5cffa933`, including bounded explicit-rate pacing.

The opt-in Chrome transport/Initial-packet profile, send-time congestion metadata
and BBR profile controls are rebased from `crapthings/hysteria-rust`
`d47b9a9b5ab9e8912919990cf11904878d9552b7` (its base is Quinn 0.11.16).
Hysteria 2.13.0 and quic-go `a10df75c260c` are the compatibility oracle.
The reference's license is retained in `LICENSE-HYSTERIA-RUST-PATCHES`.

Chrome shaping is opt-in. Ordinary connections retain minimum-MTU padding and
window pacing. HY2 configures BBR's profiles, 32-packet initial window,
20,000-packet maximum and MTU scaling; ordinary callers retain byte-window defaults.
The controller API carries send/loss metadata, a per-event prior flight size and
one fresh RTT callback per ACK frame. Actual controller implementations and
consumers must be compiled against the new API together.

The optional `hy2-rustls` feature exposes authenticated ECH/ALPS metadata from
EasyTier's separately maintained rustls patch. Enabling it requires that explicit
TLS dependency override. Ordinary Quinn builds continue to use official rustls.

The candidate passes protocol unit tests and the ignored Go first-flight wire
comparison on the dedicated build host. The Go probe is test-only and pinned in
`tools/chrome-wire-reference`; set `HY2_CHROME_GO_PROBE` when running the ignored
test. Consumer integration, cross-platform runtime, capacity comparisons and
rollout remain separate release gates. This document does not assert deployment.
