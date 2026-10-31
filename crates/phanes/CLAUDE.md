# CLAUDE.md — phanes

Workspace-wide rules (coding standards, commands, what goes in `layline`) are in the root `CLAUDE.md`.

## Background

Phanes is the control center of the trading system, deployed on **RedmiBook** (named after Phanes, "the Heavenly Principles" in Genshin Impact). Two services run in one process on one port:

- **Gnosis** (API key release; the Heavenly Principles hand out Gnoses): programs on Osaka ask Gnosis for API keys through the Cloudflare Tunnel when they start.
- **Celestia** (process monitoring; Celestia watches over Teyvat, plan in `plan/celestia.md`): Osaka reports the CPU, memory and network of business processes every second over WebSocket. For now Phanes only logs them and acks; storage (ClickHouse, see `plan/database.md`) and dashboards come later.

The production Osaka-side clients live in other crates (`market_data_provider` fetches its key through Gnosis); `examples/` here simulates Osaka for testing.

## Requirements

- Osaka ↔ RedmiBook traffic is **end-to-end encrypted**: requests and replies are ciphertext. Cloudflare terminates TLS and sees HTTP in plaintext, so encryption must happen at the application layer, not rely on TLS.
- Both sides already hold each other's SSH public key (ed25519).
- Both ends are Rust (tokio).
- RedmiBook's own security is out of scope for now: API keys are plaintext in `api_keys/*.txt` under this crate (e.g. `api_keys/binance_demo_trading.txt`, a dummy key). `api_keys/` is gitignored; **no key may ever enter git, logs or example code**.

## Decisions

- Gnosis protocol: Noise `Noise_KK_25519_ChaChaPoly_BLAKE2s` (`snow`), prologue `gnosis`, handshake plus request/reply in one HTTP POST. Message 1 carries `{key_id, timestamp, nonce}`, message 2 carries the API key.
- Celestia protocol: same Noise parameters, prologue `celestia`, `GET /celestia` upgraded to WebSocket. The first two frames are the handshake (empty payloads); every later frame is one postcard-encoded ciphertext: Osaka → Phanes `Batch{sequence, samples}`, Phanes → Osaka `Ack{sequence}`. One handshake per connection; no time window or nonce needed.
- Keys: each service has its own X25519 key pair on each side (not the SSH keys, and not shared between services); public keys are exchanged over the existing SSH channel.
- Gnosis and Celestia are both required: if either service's config is missing or wrong (invalid key, permissions not 600), Phanes refuses to start.
- IP allow list (mounted in release builds only): one `[clients.<name>]` table per client, `ipv4` required and `ipv6` optional; each service's `allowed_clients` references names; `GET /info` is never filtered. The allow list is attached only to each service's routes (`route_layer`); unknown paths get 404 and a full request log entry, never a push. The client IP comes only from `CF-Connecting-IP` (never `X-Forwarded-For`); a missing header, a parse failure or an address outside the list all get an empty 404 and a Bark push (throttled to once per 10 minutes). Blocked and unauthenticated requests log every header and the hex of the first 4096 bytes of the payload (body, or the offending WebSocket frame); the push has no room for that and carries only method, path, `User-Agent` / `Upgrade` / `CF-Ray` / `CF-IPCountry` / `CF-Worker` and the payload length. `listen_address` must be loopback. An unknown name, an invalid IP, the same IP under two names, or an empty `allowed_clients` refuses to start. Cloudflare adds a WAF rule as a second layer.
- A failed Celestia handshake or a frame that fails to decrypt (wrong key or forged frame) is a security event: disconnect and push via Bark (throttled to once per 10 minutes). Other runtime errors (disconnects, timeouts, malformed payloads) only drop the connection and log, without a push; Osaka reconnects and resends.
- Osaka fetches each key once at startup and keeps it in memory.
- There is only one client (Osaka) for now; generalize when requirements change, not before.
- Every key granted or refused is pushed to the phone through encrypted Bark (`layline::bark`, AES-256-CBC). Bark credentials are in the `[bark]` table of `phanes.toml` (`device_key` / `encryption_key`, optional `server`). All config comes from `phanes.toml`, which must be chmod 600 and is never committed.
- Each service uses its own key pair and names (Gnosis: `gnosis.key` / `gnosis.pub`, `gnosis_private_key`; Celestia: `celestia.key` / `celestia.pub`, `celestia_private_key`), never a generic `server_*`.

## Code structure

- One directory per service. The Gnosis protocol is shared with its clients, so both halves (`Request`/`Response`, `Initiator`/`Responder`) live in `layline::gnosis`; only the server side is here. The Celestia protocol has no client outside this crate yet, so `celestia/proto.rs` stays here; move it to layline when a second crate needs it.
- `layline::noise`: the Noise layer every service shares: parameters, key encoding, `build_handshake(prologue, ..)`, `encrypt`/`decrypt` (for handshake and transport alike). Service protocol code never allocates buffers and calls `snow` itself; it goes through this module.
- `src/gnosis/server.rs`: `GnosisState`, `POST /gnosis`. Validation order: Noise handshake → time window → nonce replay check → key_id format → ACL → read file. A failed handshake gets an empty 404 (push throttled to once per 10 minutes); a refusal after authentication gets an encrypted `Denied{reason}`.
- `src/celestia/`: `proto.rs` (`ProcessSample`/`Batch`/`Ack`, `Initiator`/`respond`/`Session`); `server.rs` (`GET /celestia`: handshake → decrypt → log → ack; a failed handshake or decryption disconnects and pushes, throttled; 90 seconds without a message disconnects).
- `src/service.rs`: the `Service` enum (Gnosis / Celestia) and `Language`; `to_string(Language)` gives the English log name or the Chinese push name. It is a leaf module, so the services can name themselves without importing `server`.
- `src/server.rs`: `router()` / `run()`, which merge every service into one axum `Router` (no allow list when `ClientFilters` is `None`; `/info` never has one), and `GET /info` (JSON: status / server_time (UTC) / version / build).
- `src/client_filter.rs`: the IP allow list middleware `filter_clients` and one `ClientFilter` per service; an allowed request carries its `ClientName` in the request extensions, which handlers use in logs and pushes (`unfiltered` when no middleware is mounted).
- `src/config.rs`: `PhanesConfig`, with `[clients]`, `gnosis_service_config` and `celestia_service_config` (all required); `parse_gnosis_clients_from_toml()` / `parse_celestia_clients_from_toml()` map `IpAddr` → name. Secret files are read through `layline::secrets`.
- `src/request_details.rs`: the snapshot of a refused request, `RequestDetails` (`RequestHead` plus payload); `Display` gives the one-line full log entry, `summarize()` the push summary.
- `src/notify.rs`: the push events (started / granted / denied / unauthenticated / blocked / crashed) and their Chinese texts; a background task delivers them through `layline::bark::Notifier`: Bark in release builds, where a missing `[bark]` logs an error and exits; in debug builds (local development) Bark is not needed and pushes become `info!` lines. Unauthenticated and blocked pushes are per `Service`, each with its own `UnauthenticatedThrottle`.
- `src/main.rs`: `phanes keygen` and `phanes serve`. serve runs under a supervisor: any panic (panic hook) or fatal error → log → push "天理陷入沉睡" → exit code 1.
- `deploy/`: example config, systemd unit and the deploy script `phanes.sh` (install keeps keys and config / uninstall removes everything / reinstall). Steps are in the README. `examples/fetch.rs`: a test client that fetches a key like Osaka; `examples/celestia/`: a simplified Osaka process monitor (`client.rs` in-memory `Outbox`, reconnect and resend, 30-second heartbeat; `sampler.rs` reads `/proc` and `systemctl show`, filling 0 when network data is unavailable). Code only tests use goes in `examples/`, not `src/`; the example's tests run with `cargo test` through `test = true` in `Cargo.toml`.
