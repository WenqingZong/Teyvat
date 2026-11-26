# AGENTS.md

Instructions for AI agents (DSH) working on Teyvat, the single repository of a personal quant
trading system (names come from Genshin Impact). Read [`README.md`](README.md) for the human
overview, then the component's file in `architecture/` before changing it. `plan/` holds designs
that are not implemented yet; `deploy/` holds the human deployment steps.

## Where things are

One Cargo workspace (edition 2024, resolver 3). The root manifest is also the `teyvat` package,
which defines **every binary and example**, so `cargo build --release --bins --examples` builds
them all. All other code lives in library crates under `crates/`:

| Crate | Role |
|---|---|
| `phanes` | Control centre: Gnosis releases API keys, Celestia receives process metrics. |
| `market_data_provider` | Connects to exchanges and publishes market data to strategies over shared memory. |
| `dummy_strategy` | Development stand-in strategy and latency bench. |
| `layline` | LayLine (地脉): the code every other crate shares. |
| `layline-testing` | Dev-only source checks shared by the tests; never a production dependency. |

Crates hold code only. Everything else lives at the top level: `architecture/` (implemented
design), `plan/` (unimplemented design), `deploy/` (deployment), `examples/` (smoke-test tools),
`src/bin/` (the service entry points). The machines and their roles are in `README.md`; details
are in [`architecture/overview.md`](architecture/overview.md).

Private strategies will later join as a git submodule outside `crates/` (e.g. `strategies/`),
listed in the workspace's `exclude` and keeping their own `Cargo.lock`.

## Documentation rules

- **README.md** is for humans: the whole system at a glance, no implementation detail.
- **`architecture/`** is for AI: how the implemented system works, its invariants and code
  structure. Read the relevant file before changing a component and update it in the same change.
- **`plan/`** is for AI: features not yet implemented and open design questions. Items marked
  【待定】 need the user's decision first. When something is implemented, move its description to
  `architecture/` and drop it from `plan/`.
- **`deploy/`** is for humans: the steps and scripts to deploy each service.
- This file is the agent's working rules. It is in English; `README.md`, `architecture/`,
  `plan/` and `deploy/README.md` are in Chinese. Crates contain no docs; no other file is named
  after an agent.
- **Current version only:** the system is in development and promises nothing to anyone, so code,
  deploy scripts and docs serve only the current version: no backward compatibility, no detection
  of old formats, no migration steps. A reader must understand what code does and why it exists
  without consulting git history; a one-off migration note belongs in the PR description.
- **Comments explain why, and must match the code.** A comment that describes an older behaviour,
  a removed field or a stale number is a bug: fix the comment, not the code.

## Commands

Run from the workspace root.

```bash
cargo build --release --bins --examples       # every binary and example (the teyvat package)
cargo clippy --workspace --all-targets
cargo fmt --all
cargo test --workspace                        # unit, integration and example tests of every crate
cargo test -p <crate> <name> -- --nocapture   # one test
cargoscope scan . --checks inventory,unused   # unused-dependency report; CI fails if any
cargo test --workspace --test module_dependencies --test import_groups --test format_strings
cargo llvm-cov --workspace --summary-only     # line coverage; CI requires 90%
cargo bench -p market_data_provider           # criterion latency benches
deploy/phanes/phanes.sh install               # deploy; see deploy/README.md
```

CI (`.github/workflows/rust_ci.yml`) runs fmt, clippy, the module cycle, import grouping and
format string checks, tests and coverage with `RUSTFLAGS=-Dwarnings`. Dependabot opens dependency
PRs daily.

## Coding standards

- **Zero diagnostics:** `cargo check --workspace --all-targets` and
  `cargo clippy --workspace --all-targets` must produce no warnings or errors, and
  `cargo fmt --all --check` must pass. The only exception is a clippy lint whose fix would need a
  large, sweeping change; leave it, and call it out explicitly. Run `cargo test --workspace`
  before declaring any change done.
- **Workspace lints** (`[workspace.lints.clippy]`, inherited by every crate through
  `[lints] workspace = true`): `unwrap_used`, `print_stdout`, `print_stderr` and `dbg_macro` are
  denied, in tests too. Use `expect("reason")` where a failure is a bug.
- **Dependencies:** versions are declared once in `[workspace.dependencies]`; crates use
  `<name>.workspace = true` and add features locally.
- **Compiler over convention:** whatever rustc, clippy or cargo can enforce is enforced by them,
  not by a comment saying "never do X": visibility, `#[cfg]`, types, lints, exhaustive matches.
  Where no compiler mechanism exists, a test enforces it.
- **No module cycles:** every crate has `tests/module_dependencies.rs`, which calls
  `layline_testing::assert_acyclic_modules`; top-level modules must not import each other in a
  cycle. Shared names such as Phanes' `Service` live in a leaf module, not in the module that
  composes everything.
- **What goes in layline:** only code that two or more crates use today; code one crate uses stays
  in that crate until a second one needs it. layline is one crate without feature flags; its
  modules are listed in `architecture/overview.md`. `layline::testing` holds the test fixtures
  every crate's tests share and is public because of that, so only convention keeps production
  code from calling it.
- **Logging:** only `tracing`. Import the macros at the top of the file
  (`use tracing::{info, warn};`) and write `info!`, not `tracing::info!`. Binaries and examples
  call `layline::logging::init_tracing()` once and keep the returned guard alive until exit,
  because dropping it flushes buffered lines.
- **Imports:** no long inline paths such as `std::fs::read_to_string` or `snow::Builder`; `use`
  them at the top of the file, and rename clashes with clear aliases (`IoError`, `TomlError`).
  Group `use` items in three blocks separated by one blank line: the standard library (`std`,
  `core`, `alloc`), then third-party crates, then this repository (`crate`, `self`, `super`,
  workspace crates such as `layline`, child modules); rustfmt sorts within each block. Every
  package's `tests/import_groups.rs` (`layline_testing::assert_grouped_imports`) enforces it.
- **Format strings:** one placeholder style per string: never mix positional (`{}`, `{:?}`,
  `{0}`) with named (`{name}`). Bind an expression such as `path.display()` to a local first, then
  inline it (`let path = path.display(); info!("loaded {path}: {config:#?}");`); in a `thiserror`
  attribute, where no local can be bound, use a named field or a named argument. Every package's
  `tests/format_strings.rs` (`layline_testing::assert_consistent_format_strings`) enforces it.
- **Naming:** names are spelled out, never abbreviated. A binding (variable, parameter, closure
  or loop variable, pattern) whose type is `Result` ends in `_result`, and one whose type is
  `Option` ends in `_option`; unwrapped values take no suffix. A timestamp or time span held as a
  raw integer names its unit (`exchange_time_nanoseconds`); `Duration` and `Instant` need none.
- **No magic numbers:** define named constants.
- **Types:** no generic parameters for show (such as `impl Into<String>`) when they buy nothing;
  keep parameters of the same kind the same type; prefer semantic types (`Url`, `SecretString`,
  `StatusCode`), even when that needs a hand-written serde adapter.
- **Errors:** `thiserror` enums for errors callers match on; `anyhow::Result` for any other error;
  never `Box<dyn Error>`.
- **Secrets:** never log a secret. Hold secrets in memory with `secrecy` (`SecretString`,
  `SecretBox<T>`), not `zeroize::Zeroizing`. Config files with credentials and private key files
  must be chmod 600 (`layline::secrets::read_secret_file` refuses anything else). No secret, real
  key or credential ever enters git, logs or examples; `deploy/phanes/phanes.example.toml` holds
  only placeholders. Noise public keys are held in `SecretBox<StaticKey>` like private keys, and
  never logged: harmless if leaked, but kept from everyone outside the allow list. Every gnosis
  (API key) Phanes releases is a `GnosisId` in `layline::gnosis`, and each id holds one
  `deny_unknown_fields` key type, defined in that exchange's file
  (`layline/src/gnosis/binance.rs`): Phanes reads them from the `[gnoses]` table of
  `phanes.toml`, so serde checks them at startup. Config parse errors never quote the file.
- **Config:** each service reads all its config from one chmod 600 TOML file through
  `layline::secrets::read_secret_toml`, never from environment variables. Strategies share one
  format, `strategy.toml`; a strategy's own tuning knobs stay command-line arguments.
- **Clock source:** wall-clock timestamps come from
  `layline::time::get_current_timestamp_nanoseconds()` (`SystemTime`, `CLOCK_REALTIME`, a
  `NonZeroU64`); hot paths use it as a plain integer and never format dates.
- **Commits:** Conventional Commits.

## Nightly test (read-only)

The nightly test belongs to the user: they start `market_data_provider` and `dummy_strategy`
themselves, normally inside a tmux session on each trading host, and they own the processes'
lifecycle. Our job is to watch the logs and report; nothing else.

**Read-only, without exception.** While a nightly test runs, never stop, start, restart or kill a
service, never edit a config, never build or copy a binary on a host, and never touch the rings
under `/dev/shm`. Only read-only SSH commands are allowed on the hosts: `ps`, `pgrep`, `curl`,
`grep`, `tail`, `free`, `df`. `pkill`, `systemctl`, `setsid`, `scp`, `cargo` and anything like
`install_teyvat` are off limits there. A missing or stopped process is **reported, never
restarted**: only the user restarts it, from their own tmux session.

Hosts and paths (SSH aliases `Osaka`, `Tokyo` exist locally):

| Host | Services | Logs |
|---|---|---|
| `Osaka` | `/home/ubuntu/Services/MarketDataProvider/`, `/home/ubuntu/Services/DummyStrategy/` | `*.log` in each directory |
| `Tokyo` | same paths | same |

Assumptions about the run, which must not be changed by the watcher: the strategy reports latency
every 30 minutes (`--report-interval-seconds=1800`, so windows compare across nights), and Osaka
runs the demo environment while Tokyo runs live; read each host's `strategy.toml` for the
exact topic set (Tokyo includes `AGGREGATE_TRADE`; demo testnet does not list AAPL/MSFT
perpetuals).

Patrol every two hours, scheduled with DSH's scheduler (`this_session`,
`FREQ=HOURLY;INTERVAL=2`), not with cron, so the reports arrive in the conversation. Per host:

```bash
ssh <host> 'pgrep -af "^\./market_data_provider --config"; pgrep -af "^\./dummy_strategy --config";
  curl -s --max-time 3 http://127.0.0.1:6666/health; echo;
  grep -cE " ERROR|panic|CRITICAL" ~/Services/*/*.log;
  tail -c 4000 ~/Services/MarketDataProvider/market_data_provider.log;
  tail -c 4000 ~/Services/DummyStrategy/dummy_strategy.log'
```

Look for, in this order: a missing process; an `ERROR`, `panic` or non-zero exit; repeated
Celestia disconnects between the hosts; topics missing from `/subscribed`; a missing 30-minute
latency report; a large p99/max regression against the previous window; memory growth; disk or
inode pressure. `WARN` alone is not an anomaly (Cloudflare resets the Celestia connection
occasionally; the client reconnects and resends).

Report concisely: what changed since the last patrol, and only anomalies with the log line that
proves them; never paste whole logs. When a process is down, say when it was last seen, quote its
last log lines, and leave it down.

Stop patrolling when the user says so. Services are the user's to keep running or stop.

## Environment

DSH itself runs on the user's Ubuntu Server as the systemd user service `dsh-web` (linger
enabled), so sessions, scheduled patrols and agent turns keep running when no browser is
connected; closing the web page on another machine does not stop them.
