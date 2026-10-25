# Maintained EasyTier dependency

`dev` is the maintained custom integration branch. Its runtime source is based on
`checkpoint/easytier-fe44e654`, commit
`fe44e654b2b2d18dd2b502931945cbdff4a96af4`. Maintenance documentation may advance
the branch without changing the dependency checkpoint.

EasyTier's Cargo.toml and Cargo.lock select this exact revision for
`quinn-proto` 0.11.12. Its `quinn` wrapper is registry version 0.11.8. Builds use
the pinned Git dependency, not a neighboring local checkout.

The existing `main` reference is retained as a separate upstream-tracking
snapshot. Advancing it or changing to a newer official Quinn release requires
its own compatibility review. Dependency cleanup does not select a new release
or retune congestion control.

## Integrated patch boundary

The checkpoint contains the custom controller pacing interface, Hysteria BBR
profiles and sampler, stale-sample expiry, GSO per-packet feedback, extended
receive history, optional learned loss thresholds and bounded explicit-rate
pacing credit. Older pacing and BBR/sampler branch tips are ancestors of this
checkpoint and need no additional merge.

These patches have different roles and should be evaluated separately when
changing the dependency. The adaptive BBR profile experiment below is distinct
from the optional adaptive loss recovery already included in the checkpoint.

## Retained experiments

| Annotated tag | Boundary |
| --- | --- |
| `archive/ack-blocks-256-138c9191` | ACK range cap from 64 to 256; one additional experimental commit. |
| `archive/adaptive-cap-128-a92bcf43` | Lower learned reorder thresholds to 128 packets / 1.25 RTT; one additional experimental commit. |
| `archive/pacer-smoothing-fe34c5c8` | Wake pacing at 200 microseconds rather than 1 millisecond; one additional experimental commit. |
| `archive/hy2-adaptive-f6581014` | Historical optional adaptive BBR profile based on the older sampler branch. |
| `archive/easytier-pacing-42cb8962` | Historical ancestor of the maintained checkpoint. |
| `archive/bbr-startup-loss-67611d64` | Historical ancestor of the maintained checkpoint. |

The additional experimental changes are preserved without selecting them for
the maintained dependency. Restore a scoped worktree from a tag, compare with
current `dev`, and validate both protocol behavior and affected EasyTier paths
before integration.

The consolidation changes documentation and Git organization only. It preserves
the exact deployed dependency source; it does not establish new performance
claims. Independent Git backup/restore evidence and historical experiment build
caches are kept in the private operations archive.
