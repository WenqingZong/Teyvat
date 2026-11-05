#!/usr/bin/env bash
# Deploy / redeploy / remove the dummy strategy on the trading host, beside the market data provider.
#
#   deploy/dummy_strategy/dummy_strategy.sh install      build and (re)deploy; keeps the existing config
#   deploy/dummy_strategy/dummy_strategy.sh uninstall    remove everything the strategy put on this machine
#   deploy/dummy_strategy/dummy_strategy.sh reinstall    uninstall, then install (config back to the example)
#
# Add --yes to skip the confirmation prompt of uninstall / reinstall.
# Run as the market data provider's user, from anywhere; sudo is used where needed.

readonly SERVICE=dummy_strategy
readonly BINARY=/usr/local/bin/dummy_strategy
readonly CONFIG_DIRECTORY=/etc/dummy_strategy
readonly CONFIG_FILE=$CONFIG_DIRECTORY/strategy.toml
# How long the strategy must stay up after a restart to count as started.
readonly STARTUP_CHECK_SECONDS=5
source "$(dirname "${BASH_SOURCE[0]}")/../common.sh"

install_service() {
    install_binary_and_config strategy.example.toml
    install_unit
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

uninstall_service() {
    warn "this deletes $CONFIG_DIRECTORY (config), $BINARY, $UNIT_FILE and the $SERVICE journal logs."
    confirm "continue?" || die "aborted"
    remove_service "$CONFIG_DIRECTORY"
    log "$SERVICE removed (its rings under /dev/shm/market_data_provider/$SERVICE belong to the provider and stay)"
}

dispatch "the market data provider's user" "$@"
