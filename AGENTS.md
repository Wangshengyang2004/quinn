# Maintained fork notes

- Use `dev` as the custom integration branch. Preserve the existing `main`
  reference and do not force-push either line.
- Read `DEVELOPMENT.md` for the maintained dependency checkpoint and archived
  experiments. Resume experiments in a scoped branch/worktree; archive tags are
  not instructions to merge their changes.
- EasyTier selects this fork through an exact `quinn-proto` Git revision in its
  Cargo.toml and Cargo.lock. A local branch change does not update that consumer.
  Validate consumer changes explicitly when advancing the dependency revision.
- Transport changes can affect ordinary QUIC as well as HY2. Verify the affected
  protocol tests and consumer paths; do not infer production benefit from an
  isolated congestion-control benchmark.
- Use optimized release builds, task-specific target directories outside the
  repository and the dedicated build node. Preserve other tasks' build caches.
- Keep machine-specific build recipes, credentials and rollout evidence outside
  the source repository. Retain a restorable backup before removing worktrees or
  duplicate repositories.
