#!/usr/bin/env bash
# Deploy / redeploy / remove the dummy strategy on the trading host, beside the market data provider.
#
#   deploy/dummy_strategy/dummy_strategy.sh install      build and (re)deploy; keeps the existing config
#   deploy/dummy_strategy/dummy_strategy.sh uninstall    remove everything the strategy put on this machine
#   deploy/dummy_strategy/dummy_strategy.sh reinstall    uninstall, then install (config back to the example)
#
# Add --yes to skip the confirmation prompt of uninstall / reinstall.
# Run as the market data provider's user, from anywhere; sudo is used where needed.

set -euo pipefail

readonly SERVICE=dummy_strategy
readonly BINARY=/usr/local/bin/dummy_strategy
readonly CONFIG_DIRECTORY=/etc/dummy_strategy
readonly CONFIG_FILE=$CONFIG_DIRECTORY/strategy.toml
readonly UNIT_FILE=/etc/systemd/system/$SERVICE.service
# How long the strategy must stay up after a restart to count as started.
readonly STARTUP_CHECK_SECONDS=5
REPOSITORY_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
readonly REPOSITORY_ROOT

log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
    [[ ${ASSUME_YES:-false} == true ]] && return 0
    read -r -p "$1 [y/N] " answer
    [[ $answer == y || $answer == Y ]]
}

install_strategy() {
    cd "$REPOSITORY_ROOT"
    command -v cargo >/dev/null || die "cargo not found; install Rust via rustup first"

    log "building release binary"
    cargo build --release -p teyvat --bin $SERVICE
    # Install beside, then rename: overwriting the running binary in place fails with "Text file busy".
    sudo install -m 755 target/release/$SERVICE "$BINARY.new"
    sudo mv "$BINARY.new" "$BINARY"

    log "preparing $CONFIG_DIRECTORY"
    sudo mkdir -p "$CONFIG_DIRECTORY"
    sudo chown -R "$USER:" "$CONFIG_DIRECTORY"
    chmod 700 "$CONFIG_DIRECTORY"
    [[ -f $CONFIG_FILE ]] || install -m 600 deploy/$SERVICE/strategy.example.toml "$CONFIG_FILE"

    log "installing systemd unit"
    sed "s/<your user>/$USER/" deploy/$SERVICE/$SERVICE.service | sudo tee "$UNIT_FILE" >/dev/null
    sudo systemctl daemon-reload
    sudo systemctl enable --quiet "$SERVICE"

    log "restarting $SERVICE"
    sudo systemctl restart "$SERVICE"
    check_running
}

# The strategy has no HTTP API; it counts as started if it is still running a few seconds later,
# past loading the config, which is where a bad config stops it.
check_running() {
    sleep "$STARTUP_CHECK_SECONDS"
    if systemctl is-active --quiet "$SERVICE"; then
        log "$SERVICE is running; latency reports: journalctl --namespace=$SERVICE -u $SERVICE -f"
        return 0
    fi
    sudo systemctl status "$SERVICE" --no-pager || true
    die "$SERVICE is not running; see: journalctl --namespace=$SERVICE -u $SERVICE -e"
}

uninstall_strategy() {
    warn "this deletes $CONFIG_DIRECTORY (config), $BINARY, $UNIT_FILE and the $SERVICE journal logs."
    confirm "continue?" || die "aborted"

    if systemctl list-unit-files "$SERVICE.service" >/dev/null 2>&1; then
        log "stopping and disabling $SERVICE"
        sudo systemctl disable --now "$SERVICE" >/dev/null 2>&1 || true
    fi
    sudo rm -f "$UNIT_FILE"
    sudo systemctl daemon-reload
    sudo systemctl reset-failed "$SERVICE" >/dev/null 2>&1 || true
    sudo rm -f "$BINARY" "$BINARY.new"
    sudo rm -rf "$CONFIG_DIRECTORY"

    log "deleting $SERVICE journal logs"
    sudo systemctl stop "systemd-journald@$SERVICE.service" "systemd-journald@$SERVICE.socket" \
        "systemd-journald-varlink@$SERVICE.socket" >/dev/null 2>&1 || true
    sudo sh -c "rm -rf /var/log/journal/*.$SERVICE /run/log/journal/*.$SERVICE"
    log "$SERVICE removed (its rings under /dev/shm/market_data_provider/$SERVICE belong to the provider and stay)"
}

main() {
    [[ $EUID -ne 0 ]] || die "run as the market data provider's user, not root"
    local command=${1:-}
    [[ ${2:-} == --yes ]] && ASSUME_YES=true
    case $command in
        install) install_strategy ;;
        uninstall) uninstall_strategy ;;
        reinstall) uninstall_strategy && install_strategy ;;
        *) die "usage: $0 install|uninstall|reinstall [--yes]" ;;
    esac
}

main "$@"
