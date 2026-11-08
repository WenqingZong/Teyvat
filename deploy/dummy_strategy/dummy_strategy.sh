#!/usr/bin/env bash
# Deploy / redeploy / remove the dummy strategy on the trading host, beside the market data provider.
#
#   deploy/dummy_strategy/dummy_strategy.sh install      build and (re)deploy; keeps the existing config,
#                                                        but copies [celestia_client] from the provider's again
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
# Holds this machine's Celestia key, which every process on the machine reports with.
readonly PROVIDER_CONFIG_FILE=/etc/market_data_provider/market_data_provider.toml
readonly TOML_KEY_LINE='^[a-z_]+[[:space:]]*='
source "$(dirname "${BASH_SOURCE[0]}")/../common.sh"

install_service() {
    install_binary_and_config strategy.example.toml
    copy_celestia_table
    install_unit
    log "restarting $SERVICE"
    sudo systemctl restart "$SERVICE"
    check_running
}

# Replaces the header and the `key = value` lines of the strategy config's [celestia_client] table with the
# provider's; comments and blank lines stay where they are. Done on every install, so the strategy picks
# up the key of a reinstalled provider. The key only passes through bash builtins and variables, never a
# command line or another file.
copy_celestia_table() {
    [[ -f $PROVIDER_CONFIG_FILE ]] \
        || die "install the market data provider first: $PROVIDER_CONFIG_FILE holds this machine's Celestia key"
    local provider_table line in_table=false strategy_lines=()
    provider_table=$(print_celestia_table <"$PROVIDER_CONFIG_FILE")
    [[ -n $provider_table ]] || die "$PROVIDER_CONFIG_FILE has no [celestia_client] table"
    [[ $provider_table != *"<"* ]] || die "fill in [celestia_client] of $PROVIDER_CONFIG_FILE first"
    while IFS= read -r line; do
        if [[ $line == "[celestia_client]" ]]; then
            in_table=true
            strategy_lines+=("$provider_table")
        elif [[ $line == "["* ]]; then
            in_table=false
            strategy_lines+=("$line")
        elif [[ $in_table == false || ! $line =~ $TOML_KEY_LINE ]]; then
            strategy_lines+=("$line")
        fi
    done <"$CONFIG_FILE"
    log "copying [celestia_client] from $PROVIDER_CONFIG_FILE"
    printf '%s\n' "${strategy_lines[@]}" >"$CONFIG_FILE"
}

# Prints the header and the `key = value` lines of the [celestia_client] table of the TOML on stdin.
print_celestia_table() {
    local line in_table=false
    while IFS= read -r line; do
        if [[ $line == "[celestia_client]" ]]; then
            in_table=true
            printf '%s\n' "$line"
        elif [[ $line == "["* ]]; then
            in_table=false
        elif [[ $in_table == true && $line =~ $TOML_KEY_LINE ]]; then
            printf '%s\n' "$line"
        fi
    done
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
