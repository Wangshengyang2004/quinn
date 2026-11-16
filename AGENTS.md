# Maintained fork notes

- Use `dev` as the custom integration branch. Preserve the existing `main`
  reference and do not force-push either line.
- Read `DEVELOPMENT.md` for the maintained dependency checkpoint and archived
  experiments. Resume experiments in a scoped branch/worktree; archive tags are
  not instructions to merge their changes.
- EasyTier selects this fork through exact Git revisions in Cargo.toml and
  Cargo.lock. Shared UDP metadata requires wrapper, proto and UDP crates at the
  same revision. A branch change does not update the consumer; validate it
  explicitly when advancing the dependency revision.
- Transport changes can affect ordinary QUIC as well as HY2. Verify the affected
  protocol tests and consumer paths; do not infer production benefit from an
  isolated congestion-control benchmark.
- Use optimized release builds and task-specific target directories outside the
  repository on authorized Linux build hosts. Follow the current host lock,
  job and memory budget in the private build runbook. Preserve other tasks' caches.
- Keep machine-specific build recipes, credentials and rollout evidence outside
  the source repository. Retain a restorable backup before removing worktrees or
  duplicate repositories.
