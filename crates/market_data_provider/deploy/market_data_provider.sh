#!/usr/bin/env bash
# Deploy / redeploy / remove the Market Data Provider on the trading host (Osaka).
#
#   crates/market_data_provider/deploy/market_data_provider.sh install      build and (re)deploy; keeps the existing key and config
#   crates/market_data_provider/deploy/market_data_provider.sh uninstall    remove everything the provider put on this machine
#   crates/market_data_provider/deploy/market_data_provider.sh reinstall    uninstall, then install (new Gnosis client key!)
#
# Add --yes to skip the confirmation prompt of uninstall / reinstall.
# Run as the user the strategies run as, from anywhere; sudo is used where needed.

set -euo pipefail

readonly SERVICE=market_data_provider
readonly BINARY=/usr/local/bin/market_data_provider
readonly CONFIG_DIRECTORY=/etc/market_data_provider
readonly GNOSIS_PRIVATE_KEY=$CONFIG_DIRECTORY/gnosis_client.key
readonly GNOSIS_PUBLIC_KEY=$CONFIG_DIRECTORY/gnosis_client.pub
readonly CONFIG_FILE=$CONFIG_DIRECTORY/market_data_provider.toml
readonly UNIT_FILE=/etc/systemd/system/$SERVICE.service
# DEFAULT_ROOT_DIRECTORY: one ring file per topic, read by the strategies.
readonly RING_DIRECTORY=/dev/shm/market_data_provider
# The crate holds deploy/ (and, for Phanes, api_keys/); the workspace root defines the binary and holds target/.
CRATE_DIRECTORY=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
readonly CRATE_DIRECTORY
WORKSPACE_ROOT=$(cd "$CRATE_DIRECTORY/../.." && pwd)
readonly WORKSPACE_ROOT

log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
    [[ ${ASSUME_YES:-false} == true ]] && return 0
    read -r -p "$1 [y/N] " answer
    [[ $answer == y || $answer == Y ]]
}

install_provider() {
    cd "$CRATE_DIRECTORY"
    command -v cargo >/dev/null || die "cargo not found; install Rust via rustup first"
    command -v openssl >/dev/null || die "openssl not found; it generates the Gnosis client key"

    log "building release binary"
    cargo build --release -p teyvat --bin market_data_provider
    # Install beside, then rename: overwriting the running binary in place fails with "Text file busy".
    sudo install -m 755 "$WORKSPACE_ROOT/target/release/market_data_provider" "$BINARY.new"
    sudo mv "$BINARY.new" "$BINARY"

    log "preparing $CONFIG_DIRECTORY"
    sudo mkdir -p "$CONFIG_DIRECTORY"
    sudo chown -R "$USER:" "$CONFIG_DIRECTORY"
    chmod 700 "$CONFIG_DIRECTORY"

    generate_gnosis_key_pair
    [[ -f $CONFIG_FILE ]] || install -m 600 deploy/market_data_provider.example.toml "$CONFIG_FILE"

    # systemd-logind deletes a user's /dev/shm files once their last session ends (RemoveIPC=yes), which
    # would pull the rings from under running strategies; a lingering user never fully logs out.
    log "enabling linger for $USER so logging out keeps the rings"
    sudo loginctl enable-linger "$USER"

    log "installing systemd unit"
    sed "s/<your user>/$USER/" deploy/$SERVICE.service | sudo tee "$UNIT_FILE" >/dev/null
    sudo systemctl daemon-reload
    sudo systemctl enable --quiet "$SERVICE"

    local missing=()
    grep -q '<Phanes URL' "$CONFIG_FILE" && missing+=("[phanes] url in $CONFIG_FILE")
    grep -q '<base64' "$CONFIG_FILE" && missing+=("[phanes] gnosis_public_key (Phanes' /etc/phanes/gnosis.pub) in $CONFIG_FILE")
    grep -q '<fill me' "$CONFIG_FILE" && missing+=("[bark] credentials in $CONFIG_FILE")
    grep -q '^demo *=' "$CONFIG_FILE" || missing+=("demo = true|false in $CONFIG_FILE (see deploy/market_data_provider.example.toml)")
    if ((${#missing[@]} > 0)); then
        warn "not starting yet; fill in:"
        printf '     - %s\n' "${missing[@]}" >&2
        warn "then run: sudo systemctl restart $SERVICE"
        print_phanes_instructions
        return 0
    fi

    log "restarting $SERVICE"
    sudo systemctl restart "$SERVICE"
    check_health
    print_phanes_instructions
}

check_health() {
    local port
    port=$(sed -n 's/^port *= *\([0-9]*\).*/\1/p' "$CONFIG_FILE")
    for _ in $(seq 10); do
        if curl -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
            log "$SERVICE is healthy on 127.0.0.1:$port: $(curl -fsS "http://127.0.0.1:$port/info")"
            return 0
        fi
        sleep 1
    done
    sudo systemctl status "$SERVICE" --no-pager || true
    die "$SERVICE did not become healthy; see: journalctl --namespace=$SERVICE -u $SERVICE -e"
}

# The provider is a Gnosis client: an X25519 key pair in Phanes' base64 format, made with openssl like
# Phanes' README does for Osaka. Keeps an existing pair so Phanes' config stays valid across redeploys.
generate_gnosis_key_pair() {
    [[ -f $GNOSIS_PRIVATE_KEY ]] && return 0
    log "generating Gnosis client key pair"
    local pem
    pem=$(mktemp)
    openssl genpkey -algorithm X25519 -out "$pem"
    (umask 077 && openssl pkey -in "$pem" -outform DER | tail -c 32 | base64 >"$GNOSIS_PRIVATE_KEY")
    openssl pkey -in "$pem" -pubout -outform DER | tail -c 32 | base64 >"$GNOSIS_PUBLIC_KEY"
    shred -u "$pem"
    [[ -s $GNOSIS_PUBLIC_KEY ]] || die "openssl did not produce a Gnosis public key"
}

print_phanes_instructions() {
    log "on the Phanes host, in [gnosis_service_config] of /etc/phanes/phanes.toml:"
    log "  public_key = \"$(cat "$GNOSIS_PUBLIC_KEY")\""
    log "  allowed_key_ids must include binance_sbe_real (live) or binance_sbe_demo (demo)"
    log "  then: sudo systemctl restart phanes"
}

uninstall_provider() {
    warn "this deletes $CONFIG_DIRECTORY (Gnosis client key, config incl. Bark credentials), $BINARY,"
    warn "$UNIT_FILE, the $SERVICE journal logs and the rings in $RING_DIRECTORY;"
    warn "stop every strategy first. Phanes will need the new Gnosis client public key after a reinstall."
    confirm "continue?" || die "aborted"

    if systemctl list-unit-files "$SERVICE.service" >/dev/null 2>&1; then
        log "stopping and disabling $SERVICE"
        sudo systemctl disable --now "$SERVICE" >/dev/null 2>&1 || true
    fi
    sudo rm -f "$UNIT_FILE"
    sudo systemctl daemon-reload
    sudo systemctl reset-failed "$SERVICE" >/dev/null 2>&1 || true
    sudo rm -f "$BINARY" "$BINARY.new"
    sudo rm -rf "$CONFIG_DIRECTORY" "$RING_DIRECTORY"

    log "deleting $SERVICE journal logs"
    sudo systemctl stop "systemd-journald@$SERVICE.service" "systemd-journald@$SERVICE.socket" \
        "systemd-journald-varlink@$SERVICE.socket" >/dev/null 2>&1 || true
    sudo sh -c "rm -rf /var/log/journal/*.$SERVICE /run/log/journal/*.$SERVICE"
    log "$SERVICE removed (linger for $USER is left on; disable it with: sudo loginctl disable-linger $USER)"
}

main() {
    [[ $EUID -ne 0 ]] || die "run as the strategies' user, not root"
    local command=${1:-}
    [[ ${2:-} == --yes ]] && ASSUME_YES=true
    case $command in
        install) install_provider ;;
        uninstall) uninstall_provider ;;
        reinstall) uninstall_provider && install_provider ;;
        *) die "usage: $0 install|uninstall|reinstall [--yes]" ;;
    esac
}

main "$@"
