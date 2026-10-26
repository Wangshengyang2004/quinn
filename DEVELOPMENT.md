# Maintained EasyTier dependency

This migration candidate starts from official `quinn-proto-0.11.19`
(`8192ed399a26a6e8f0aba43d10ec8badaa5b905e`), with the matching Quinn 0.11.12
wrapper and quinn-udp 0.5.16. The previous production checkpoint remains
`checkpoint/easytier-fe44e654`; no deployment is implied by this branch.

The retained patch boundary is BBR application-limited startup correctness and
an optional controller byte-rate pacing interface with bounded credit for
Brutal. Hysteria profiles, the replacement sampler and per-packet GSO hooks,
adaptive loss thresholds and extended receive history are retired. Upstream
BBR and Cubic retain upstream window-based pacing.

EasyTier selects an exact Git revision in Cargo.toml and Cargo.lock. Updating
this branch alone does not update its consumer. Validate protocol and consumer
paths before changing the pin or deploying.

Historical experiments remain under the existing `archive/*` tags. Restore them
in a scoped worktree for comparison; archived patches are not integration
instructions. `dev` is the integration branch; preserve `main` and its history.
