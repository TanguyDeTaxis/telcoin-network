# Commands to Run - Observer Diagnostics

Verification commands for the observer diagnostics changes. Run these
to validate the implementation before creating PRs.

## 1. Unit tests (broadcast lag fix)

```bash
cargo test -p tn-types sync::tests -- --nocapture
```

Expected: 3 tests pass (`test_broadcast_lag_does_not_return_none`,
`test_broadcast_closed_returns_none`, `test_broadcast_try_recv_lagged`).

## 2. Health endpoint tests

```bash
cargo test -p tn-node health::tests -- --nocapture
```

Expected: 3 tests pass (`test_tcp_healthcheck_syncing`,
`test_tcp_healthcheck_behind`, `test_sync_status_body`).

## 3. Full crate checks (compile + existing tests)

```bash
cargo check -p tn-types -p tn-node -p tn-state-sync -p tn-network-libp2p -p tn-consensus-executor
cargo test -p tn-types -p tn-node
```

## 4. E2E observer tests (slow, requires binary build)

```bash
cargo test -p e2e-tests test_observer_late_join_catchup -- --ignored --nocapture
cargo test -p e2e-tests test_observer_reconnect_after_pause -- --ignored --nocapture
```

These build the `telcoin-network` binary and spin up a local testnet.
Expect several minutes per test.

## 5. Clippy

```bash
cargo clippy -p tn-types -p tn-node -p tn-state-sync -p tn-network-libp2p -p tn-consensus-executor -- -D warnings
```

## Files changed

| Commit | Files |
|--------|-------|
| feat: observer diagnostics metrics and broadcast lag fix | `crates/types/src/sync.rs`, `crates/state-sync/src/consensus.rs`, `crates/state-sync/src/lib.rs`, `crates/consensus/executor/src/subscriber.rs`, `crates/network-libp2p/src/peers/manager.rs`, `crates/network-libp2p/src/peers/cache.rs` |
| feat: sync-aware healthcheck endpoint | `crates/node/src/health.rs`, `crates/node/src/manager.rs` |
| test: broadcast channel lag handling unit tests | `crates/types/src/sync.rs` |
| test: observer e2e tests | `crates/e2e-tests/tests/it/restarts.rs` |
