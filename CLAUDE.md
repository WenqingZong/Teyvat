# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

Teyvat is the single repository of a personal quant trading system. Names come from Genshin Impact. It is one Cargo workspace (edition 2024, resolver 3). The root manifest is also the `teyvat` package, which defines **every binary and example** of the system, so `cargo build --release --bins --examples` builds them all:

| Target | Source | Deployed on |
|---|---|---|
| bin `phanes` | `src/bin/phanes.rs` | RedmiBook |
| bin `market_data_provider` | `src/bin/market_data_provider.rs` | Osaka |
| bin `dummy_strategy` | `src/bin/dummy_strategy.rs` | Osaka |
| examples `fetch`, `bark_push`, `celestia` | `examples/` (test tools that simulate Osaka) | built on RedmiBook, copied where needed |

All other code lives in library crates under `crates/`:

| Crate | Role |
|---|---|
| `phanes` | Control center: Gnosis releases API keys, Celestia receives process metrics. |
| `market_data_provider` | Connects to exchanges and publishes market data to strategies over shared memory. |
| `dummy_strategy` | Development stand-in strategy and latency bench; reads market data through `layline`. |
| `layline` | LayLine (地脉): the code every other crate shares. |

Crates hold code only. Everything else lives at the top level:

| Directory | Contents |
|---|---|
| `architecture/` | How the **implemented** system works: `overview.md` (machines, crates, layline, shared conventions), one file per component (`phanes.md`, `market_data_provider.md`, `dummy_strategy.md`). Read the relevant file before changing a component, and update it in the same change. |
| `plan/` | Features **not yet implemented** and open design questions (`celestia.md`, `database.md`, `market_data_provider.md`, `todo.md`). Items marked 【待定】 need the user's decision first. When something is implemented, move its description to `architecture/` and mark it done here. |
| `deploy/` | One subdirectory per service (`phanes/`, `market_data_provider/`, `dummy_strategy/`, `clickhouse/`) with its deploy script, systemd unit and example config, plus the deployment guide `deploy/README.md`. Scripts run from the repository root. |

Because one package holds every binary, building any one of them (e.g. a deploy's `cargo build --release -p teyvat --bin phanes`) compiles every crate.

The machines (details in `architecture/overview.md`):

- **Osaka**: Oracle Cloud free AMD instance in Osaka (1/8 OCPU, 1 GB RAM, 0.48 Gbps, static public IPv4 and static public IPv6, Ubuntu 26.04 x86_64). The **test environment** for verifying correctness; it runs the trading processes now and never stores API keys on disk.
- **Tokyo**: Oracle Cloud free ARM instance in Tokyo (4 Neoverse-N1 cores, 24 GB RAM, 0.48 Gbps per core, static public IPv4 and static public IPv6, Ubuntu 26.04 **aarch64**). The **production environment**, not yet in use.
- **Xian**: a cheap NAT cloud server in Xi'an, China, from Jiangsu Security Cloud (2 vCPU, 2 GB RAM, unknown bandwidth, 1000 GB traffic per month, static public IPv4, no public IPv6, Ubuntu 24.04 x86_64). The **benchmark machine**, only for the system's internal latency; not yet in use.
- **RedmiBook**: a home laptop (Kali Linux) without a public IP, reachable through a Cloudflare Tunnel, which runs Phanes and, later, the ClickHouse database.

Development happens in WSL2 (Ubuntu 24.04) on a separate Windows 11 laptop (Intel Core Ultra 5 235U).

Private strategies will later join as a git submodule outside `crates/` (e.g. `strategies/`), listed in the workspace's `exclude` and keeping their own `Cargo.lock`, so their names and dependencies never appear in this repository's lockfile.

## What goes in layline

Only code that two or more crates use today; code one crate uses stays in that crate until a second one needs it. layline is one crate without feature flags. Its modules are listed in `architecture/overview.md`. `layline::testing` (test fixtures and checks) is public because other crates' tests need it, so nothing but convention keeps production code from calling it.

## Commands

Run from the workspace root.

```bash
cargo build --release --bins --examples       # every binary and example (the teyvat package)
cargo clippy --workspace --all-targets
cargo fmt --all
cargo test --workspace                        # unit, integration and example tests of every crate
cargo test -p <crate> <name> -- --nocapture   # one test
cargo test --workspace --test module_dependencies --test import_groups --test format_strings   # module cycle, import grouping and format string checks
cargo llvm-cov --workspace --summary-only     # line coverage; CI requires 90%
cargo bench -p market_data_provider           # criterion latency benches
deploy/phanes/phanes.sh install               # deploy; see deploy/README.md
```

CI (`.github/workflows/rust_ci.yml`) runs fmt, clippy, the module cycle, import grouping and format string checks, tests and coverage with `RUSTFLAGS=-Dwarnings`. Dependabot opens dependency PRs daily.

## Coding standards

- **Zero diagnostics:** `cargo check --workspace --all-targets` and `cargo clippy --workspace --all-targets` must produce no warnings or errors, and `cargo fmt --all --check` must pass. The only exception is a clippy lint whose fix would need a large, sweeping change; leave it, and call it out explicitly. Run `cargo test --workspace` before declaring any change done.
- **Workspace lints** (`[workspace.lints.clippy]`, inherited by every crate through `[lints] workspace = true`): `unwrap_used`, `print_stdout`, `print_stderr` and `dbg_macro` are denied, in tests too. Use `expect("reason")` where a failure is a bug.
- **Dependencies:** versions are declared once in `[workspace.dependencies]`; crates use `<name>.workspace = true` and add features locally.
- **Compiler over convention:** whatever rustc, clippy or cargo can enforce is enforced by them, not by a comment saying "never do X": visibility, `#[cfg]`, types, lints, exhaustive matches. Where no compiler mechanism exists, a test enforces it.
- **No module cycles:** every crate has `tests/module_dependencies.rs`, which calls `layline::testing::assert_acyclic_modules`; top-level modules must not import each other in a cycle. Shared names such as Phanes' `Service` live in a leaf module, not in the module that composes everything.
- **Logging:** only `tracing`. Import the macros at the top of the file (`use tracing::{info, warn};`) and write `info!`, not `tracing::info!`. Binaries and examples call `layline::logging::init_tracing()` once and keep the returned guard alive until exit, because dropping it flushes buffered lines.
- **Imports:** no long inline paths such as `std::fs::read_to_string` or `snow::Builder`; `use` them at the top of the file, and rename clashes with clear aliases (`IoError`, `TomlError`). Group `use` items in three blocks separated by one blank line: the standard library (`std`, `core`, `alloc`), then third-party crates, then this repository (`crate`, `self`, `super`, workspace crates such as `layline`, child modules); rustfmt sorts within each block. Every package's `tests/import_groups.rs` (`layline::testing::assert_grouped_imports`) enforces it.
- **Format strings:** one placeholder style per string: never mix positional (`{}`, `{:?}`, `{0}`) with named (`{name}`). Bind an expression such as `path.display()` to a local first, then inline it (`let path = path.display(); info!("loaded {path}: {config:#?}");`); in a `thiserror` attribute, where no local can be bound, use a named field or a named argument. Every package's `tests/format_strings.rs` (`layline::testing::assert_consistent_format_strings`) enforces it.
- **Naming:** names are spelled out, never abbreviated. A binding (variable, parameter, closure or loop variable, pattern) whose type is `Result` ends in `_result`, and one whose type is `Option` ends in `_option`; unwrapped values take no suffix. A timestamp or time span held as a raw integer names its unit (`exchange_time_nanoseconds`); `Duration` and `Instant` need none.
- **No magic numbers:** define named constants.
- **Types:** no generic parameters for show (such as `impl Into<String>`) when they buy nothing; keep parameters of the same kind the same type; prefer semantic types (`Url`, `SecretString`, `StatusCode`), even when that needs a hand-written serde adapter.
- **Errors:** `thiserror` enums for errors callers match on; `anyhow::Result` for any other error; never `Box<dyn Error>`.
- **Secrets:** never log a secret. Hold secrets in memory with `secrecy` (`SecretString`, `SecretBox<T>`), not `zeroize::Zeroizing`. Config files with credentials and private key files must be chmod 600 (`layline::secrets::read_secret_file` refuses anything else). No secret, real key or credential ever enters git, logs or examples; `deploy/phanes/phanes.example.toml` holds only placeholders. Noise public keys are held in `SecretBox<StaticKey>` like private keys, and never logged: harmless if leaked, but kept from everyone outside the allow list. Every gnosis (API key) Phanes releases is a `GnosisId` in `layline::gnosis`, and each id holds one `deny_unknown_fields` key type, defined in that exchange's file (`layline/src/gnosis/binance.rs`): Phanes reads them from the `[gnoses]` table of `phanes.toml`, so serde checks them at startup, and clients fetch and parse a gnosis by its type. Config parse errors never quote the file.
- **Config:** each service reads all its config from one chmod 600 TOML file through `layline::secrets::read_secret_toml`, never from environment variables. Strategies share one format, `strategy.toml` (`strategy_name`, `provider_address`, `demo`, `process_metrics_interval_seconds`, a `[celestia_client]` table that the install copies from the provider's config, `topics` nested as exchange, instrument, market data types); a strategy's own tuning knobs stay command-line arguments.
- **Clock source:** wall-clock timestamps come from `layline::time::get_current_timestamp_nanoseconds()` (`SystemTime`, `CLOCK_REALTIME`, a `NonZeroU64`), which every crate shares; hot paths use it as a plain integer and never format dates.
- **Current version only:** the system is in development and promises nothing to anyone, so code, deploy scripts and docs serve only the current version: no backward compatibility, no detection of old formats, no migration steps. A reader must understand what code does and why it exists without consulting git history; a one-off migration note belongs in the PR description.
- **Commits:** Conventional Commits.
- **Docs:** `CLAUDE.md` is in English; `README.md`, `architecture/`, `plan/` and `deploy/README.md` are in Chinese. Crates contain no docs; only this file is CLAUDE.md.
