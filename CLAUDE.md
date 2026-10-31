# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

Teyvat is the single repository of a personal quant trading system. Names come from Genshin Impact. It is one Cargo workspace (edition 2024, resolver 3); every crate lives in `crates/`:

| Crate | Role | Deployed on |
|---|---|---|
| `phanes` | Control center: Gnosis releases API keys, Celestia receives process metrics. See `crates/phanes/CLAUDE.md`. | RedmiBook |
| `market_data_provider` | Connects to exchanges and publishes market data to strategies over shared memory. See `crates/market_data_provider/CLAUDE.md`. | Osaka |
| `dummy_strategy` | Development stand-in strategy and latency bench; reads market data through `layline`. | Osaka |
| `layline` | LayLine (地脉): the code every other crate shares. Library only. | — |

The two machines: **Osaka** is an Oracle Cloud free AMD instance (1/8 OCPU, 1 GB RAM) with a static public IPv4, which runs the trading processes and never stores API keys on disk. **RedmiBook** is a home laptop (Kali Linux) without a public IP, reachable through a Cloudflare Tunnel, which runs Phanes.

Private strategies will later join as a git submodule outside `crates/` (e.g. `strategies/`), listed in the workspace's `exclude` and keeping their own `Cargo.lock`, so their names and dependencies never appear in this repository's lockfile.

## What goes in layline

Only code that two or more crates use today; code one crate uses stays in that crate until a second one needs it. layline is one crate without feature flags. Its top-level modules:

- Market data contract: `types` (one type per file), `shared_memory` (ring format, `MarketDataReader`, `MarketDataWriter`), `provider_api` (the provider's HTTP contract with strategies).
- Phanes protocols: `noise` (Noise_KK layer), `gnosis` (both halves of the API key protocol, so the server and its clients cannot drift apart).
- Service plumbing: `bark` (encrypted push and `Notifier`), `secrets` (chmod 600 secret files), `logging` (`init_tracing`), `build` (`RELEASE_BUILD`, `build_profile`), `time` (wall clock, `/info` time format), `backoff`.
- `testing`: fixtures and checks for the tests of every crate (`test_keypair`, `instrument`, `topic`, `bbo`, `assert_acyclic_modules`, ...). Production code never calls it.

## Commands

Run from the workspace root.

```bash
cargo build                                   # every crate
cargo clippy --workspace --all-targets
cargo fmt --all
cargo test --workspace                        # unit, integration and example tests of every crate
cargo test -p <crate> <name> -- --nocapture   # one test
cargo test --workspace --test module_dependencies   # import cycle check of every crate
cargo llvm-cov --workspace --summary-only     # line coverage; CI requires 90%
```

CI (`.github/workflows/rust_ci.yml`) runs fmt, clippy, the cycle check, tests and coverage with `RUSTFLAGS=-Dwarnings`. Dependabot opens dependency PRs daily.

## Coding standards

- **Zero diagnostics:** `cargo check --workspace --all-targets` and `cargo clippy --workspace --all-targets` must produce no warnings or errors, and `cargo fmt --all --check` must pass. The only exception is a clippy lint whose fix would need a large, sweeping change; leave it, and call it out explicitly. Run `cargo test --workspace` before declaring any change done.
- **Workspace lints** (`[workspace.lints.clippy]`, inherited by every crate through `[lints] workspace = true`): `unwrap_used`, `print_stdout`, `print_stderr` and `dbg_macro` are denied, in tests too. Use `expect("reason")` where a failure is a bug.
- **Dependencies:** versions are declared once in `[workspace.dependencies]`; crates use `<name>.workspace = true` and add features locally.
- **No module cycles:** every crate has `tests/module_dependencies.rs`, which calls `layline::testing::assert_acyclic_modules`; top-level modules must not import each other in a cycle. Shared names such as Phanes' `Service` live in a leaf module, not in the module that composes everything.
- **Logging:** only `tracing`. Import the macros at the top of the file (`use tracing::{info, warn};`) and write `info!`, not `tracing::info!`. Binaries and examples call `layline::logging::init_tracing()` once and keep the returned guard alive until exit, because dropping it flushes buffered lines.
- **Imports:** no long inline paths such as `std::fs::read_to_string` or `snow::Builder`; `use` them at the top of the file, and rename clashes with clear aliases (`IoError`, `TomlError`).
- **Naming:** names are spelled out, never abbreviated. A binding (variable, parameter, closure or loop variable, pattern) whose type is `Result` ends in `_result`, and one whose type is `Option` ends in `_option`; unwrapped values take no suffix. A timestamp or time span held as a raw integer names its unit (`exchange_time_nanoseconds`); `Duration` and `Instant` need none.
- **No magic numbers:** define named constants.
- **Types:** no generic parameters for show (such as `impl Into<String>`) when they buy nothing; keep parameters of the same kind the same type; prefer semantic types (`Url`, `SecretString`, `StatusCode`), even when that needs a hand-written serde adapter.
- **Errors:** `thiserror` enums for errors callers match on; `anyhow::Result` for any other error; never `Box<dyn Error>`.
- **Secrets:** never log a secret. Hold secrets in memory with `secrecy` (`SecretString`, `SecretBox<T>`), not `zeroize::Zeroizing`. Config files with credentials and private key files must be chmod 600 (`layline::secrets::read_secret_file` refuses anything else). No secret, real key or credential ever enters git, logs or examples; `api_keys/`, `phanes.toml` and `market_data_provider.toml` are gitignored.
- **Config:** each service reads all its config from one TOML file, never from environment variables.
- **Clock source:** wall-clock timestamps come from `layline::time::get_current_timestamp_nanoseconds()` (`SystemTime`, `CLOCK_REALTIME`, a `NonZeroU64`), which every crate shares; hot paths use it as a plain integer and never format dates.
- **Commits:** Conventional Commits.
- **Docs:** `CLAUDE.md` files are in English; `README.md` files are in Chinese.
