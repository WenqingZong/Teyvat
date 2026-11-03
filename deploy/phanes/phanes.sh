#!/usr/bin/env bash
# Deploy / redeploy / remove Phanes (the Control Center for Quant Trading Bot) on RedmiBook.
#
#   deploy/phanes/phanes.sh install      build and (re)deploy; keeps existing keys and config
#   deploy/phanes/phanes.sh uninstall    remove everything phanes put on this machine
#   deploy/phanes/phanes.sh reinstall    uninstall, then install (new Gnosis and Celestia keys!)
#
# Add --yes to skip the confirmation prompt of uninstall / reinstall.
# Run as your normal user from anywhere; sudo is used where needed.

set -euo pipefail

readonly BINARY=/usr/local/bin/phanes
readonly CONFIG_DIRECTORY=/etc/phanes
readonly GNOSES_DIRECTORY=$CONFIG_DIRECTORY/gnoses
readonly GNOSIS_PRIVATE_KEY=$CONFIG_DIRECTORY/gnosis.key
readonly GNOSIS_PUBLIC_KEY=$CONFIG_DIRECTORY/gnosis.pub
readonly CELESTIA_PRIVATE_KEY=$CONFIG_DIRECTORY/celestia.key
readonly CELESTIA_PUBLIC_KEY=$CONFIG_DIRECTORY/celestia.pub
readonly CONFIG_FILE=$CONFIG_DIRECTORY/phanes.toml
readonly UNIT_FILE=/etc/systemd/system/phanes.service
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

install_phanes() {
    cd "$REPOSITORY_ROOT"
    command -v cargo >/dev/null || die "cargo not found; install Rust via rustup first"
    compgen -G "gnoses/*.json" >/dev/null || die "no gnoses/*.json in the repository; nothing to serve"

    log "building release binary"
    cargo build --release -p teyvat --bin phanes
    sudo install -m 755 target/release/phanes "$BINARY"

    log "preparing $CONFIG_DIRECTORY"
    sudo mkdir -p "$GNOSES_DIRECTORY"
    sudo chown -R "$USER:" "$CONFIG_DIRECTORY"
    chmod 700 "$CONFIG_DIRECTORY" "$GNOSES_DIRECTORY"

    generate_key_pair Gnosis "$GNOSIS_PRIVATE_KEY" "$GNOSIS_PUBLIC_KEY"
    generate_key_pair celestia "$CELESTIA_PRIVATE_KEY" "$CELESTIA_PUBLIC_KEY"

    # log "syncing gnoses/*.json into $GNOSES_DIRECTORY"
    # install -m 600 gnoses/*.json "$GNOSES_DIRECTORY/"

    [[ -f $CONFIG_FILE ]] || install -m 600 deploy/phanes/phanes.example.toml "$CONFIG_FILE"

    log "installing systemd unit"
    sed "s/<your user>/$USER/" deploy/phanes/phanes.service | sudo tee "$UNIT_FILE" >/dev/null
    sudo systemctl daemon-reload
    sudo systemctl enable --quiet phanes

    local missing=()
    grep -q '<base64' "$CONFIG_FILE" && missing+=("Osaka public keys (gnosis / celestia_service_config) in $CONFIG_FILE")
    grep -q '<fill me' "$CONFIG_FILE" && missing+=("[bark] credentials in $CONFIG_FILE")
    grep -q '<Osaka IP' "$CONFIG_FILE" && missing+=("Osaka's IPv4 in [clients.Osaka] of $CONFIG_FILE, and its IPv6 (or delete that optional line)")
    grep -q '^\[clients\.' "$CONFIG_FILE" && grep -q '^allowed_clients' "$CONFIG_FILE" && ! grep -q '^name *=' "$CONFIG_FILE" \
        || missing+=("[clients.<name>] tables and allowed_clients in $CONFIG_FILE (see deploy/phanes/phanes.example.toml)")
    grep -q '^celestia_private_key_file' "$CONFIG_FILE" && grep -q '^\[celestia_service_config\]' "$CONFIG_FILE" \
        || missing+=("celestia_private_key_file and [celestia_service_config] in $CONFIG_FILE (see deploy/phanes/phanes.example.toml)")
    if ((${#missing[@]} > 0)); then
        warn "not starting yet; fill in:"
        printf '     - %s\n' "${missing[@]}" >&2
        warn "then run: sudo systemctl restart phanes"
        print_public_keys
        return 0
    fi

    log "restarting phanes"
    sudo systemctl restart phanes
    check_health
    print_public_keys
}

check_health() {
    local address
    address=$(sed -n 's/^listen_address *= *"\(.*\)".*/\1/p' "$CONFIG_FILE")
    for _ in $(seq 10); do
        if curl -fsS "http://$address/info" >/dev/null 2>&1; then
            log "phanes is healthy on $address"
            return 0
        fi
        sleep 1
    done
    sudo systemctl status phanes --no-pager || true
    die "phanes did not become healthy; see: journalctl --namespace=phanes -u phanes -e"
}

# Keeps an existing key pair so Osaka's config stays valid across redeploys.
generate_key_pair() {
    local service=$1 private_key=$2 public_key=$3
    [[ -f $private_key ]] && return 0
    log "generating $service key pair"
    "$BINARY" keygen --private-key-file "$private_key" 2>&1 \
        | sed -n 's/.*public key: //p' >"$public_key"
    [[ -s $public_key ]] || die "keygen did not print a $service public key"
}

print_public_keys() {
    log "give these to Osaka:"
    log "  Gnosis public key:    $(cat "$GNOSIS_PUBLIC_KEY")"
    log "  Celestia public key: $(cat "$CELESTIA_PUBLIC_KEY")"
}

uninstall_phanes() {
    warn "this deletes $CONFIG_DIRECTORY (Gnosis and Celestia keys, config incl. Bark credentials, deployed API keys),"
    warn "$BINARY, $UNIT_FILE and the phanes journal logs."
    warn "Osaka will need the new Gnosis and Celestia public keys after a reinstall."
    confirm "continue?" || die "aborted"

    if systemctl list-unit-files phanes.service >/dev/null 2>&1; then
        log "stopping and disabling phanes"
        sudo systemctl disable --now phanes >/dev/null 2>&1 || true
    fi
    sudo rm -f "$UNIT_FILE"
    sudo systemctl daemon-reload
    sudo systemctl reset-failed phanes >/dev/null 2>&1 || true
    sudo rm -f "$BINARY"
    sudo rm -rf "$CONFIG_DIRECTORY"

    log "deleting phanes journal logs"
    sudo systemctl stop systemd-journald@phanes.service systemd-journald@phanes.socket \
        systemd-journald-varlink@phanes.socket >/dev/null 2>&1 || true
    sudo sh -c 'rm -rf /var/log/journal/*.phanes /run/log/journal/*.phanes'
    log "phanes removed (the cloudflared hostname is left untouched)"
}

main() {
    [[ $EUID -ne 0 ]] || die "run as your normal user, not root"
    local command=${1:-}
    [[ ${2:-} == --yes ]] && ASSUME_YES=true
    case $command in
        install) install_phanes ;;
        uninstall) uninstall_phanes ;;
        reinstall) uninstall_phanes && install_phanes ;;
        *) die "usage: $0 install|uninstall|reinstall [--yes]" ;;
    esac
}

main "$@"
