# Sourced by every deploy script after it sets SERVICE, BINARY, CONFIG_DIRECTORY and CONFIG_FILE:
# logging, prompts, building and installing the binary, systemd, removal, and X25519 keys in configs.

set -euo pipefail

readonly UNIT_FILE=/etc/systemd/system/$SERVICE.service
# A raw X25519 key, private or public, is the last 32 bytes of its DER encoding; a private key's DER is
# this PKCS#8 prefix followed by the raw key.
readonly X25519_KEY_BYTES=32
readonly X25519_PRIVATE_KEY_DER_PREFIX='\x30\x2e\x02\x01\x00\x30\x05\x06\x03\x2b\x65\x6e\x04\x22\x04\x20'
REPOSITORY_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
readonly REPOSITORY_ROOT

log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
    [[ ${ASSUME_YES:-false} == true ]] && return 0
    read -r -p "$1 [y/N] " answer
    [[ $answer == y || $answer == Y ]]
}

# Builds the release binary and installs it, then prepares the config directory and copies the example
# config $1 there unless a config already exists.
install_binary_and_config() {
    cd "$REPOSITORY_ROOT"
    command -v cargo >/dev/null || die "cargo not found; install Rust via rustup first"

    log "building release binary"
    cargo build --release -p teyvat --bin "$SERVICE"
    # Install beside, then rename: overwriting the running binary in place fails with "Text file busy".
    sudo install -m 755 "target/release/$SERVICE" "$BINARY.new"
    sudo mv "$BINARY.new" "$BINARY"

    log "preparing $CONFIG_DIRECTORY"
    sudo mkdir -p "$CONFIG_DIRECTORY"
    sudo chown -R "$USER:" "$CONFIG_DIRECTORY"
    chmod 700 "$CONFIG_DIRECTORY"
    [[ -f $CONFIG_FILE ]] || install -m 600 "deploy/$SERVICE/$1" "$CONFIG_FILE"
}

install_unit() {
    log "installing systemd unit"
    sed "s/<your user>/$USER/" "deploy/$SERVICE/$SERVICE.service" | sudo tee "$UNIT_FILE" >/dev/null
    sudo systemctl daemon-reload
    sudo systemctl enable --quiet "$SERVICE"
}

# Lists the placeholders still in the config, given as arguments, and fails if there are any.
report_missing() {
    (($# > 0)) || return 0
    warn "not starting yet; fill in:"
    printf '     - %s\n' "$@" >&2
    warn "then run: sudo systemctl restart $SERVICE"
    return 1
}

# Stops and removes the service: its unit, its binary, the paths given as arguments and its journal logs.
remove_service() {
    if systemctl list-unit-files "$SERVICE.service" >/dev/null 2>&1; then
        log "stopping and disabling $SERVICE"
        sudo systemctl disable --now "$SERVICE" >/dev/null 2>&1 || true
    fi
    sudo rm -f "$UNIT_FILE"
    sudo systemctl daemon-reload
    sudo systemctl reset-failed "$SERVICE" >/dev/null 2>&1 || true
    sudo rm -f "$BINARY" "$BINARY.new"
    sudo rm -rf "$@"

    log "deleting $SERVICE journal logs"
    sudo systemctl stop "systemd-journald@$SERVICE.service" "systemd-journald@$SERVICE.socket" \
        "systemd-journald-varlink@$SERVICE.socket" >/dev/null 2>&1 || true
    sudo sh -c "rm -rf /var/log/journal/*.$SERVICE /run/log/journal/*.$SERVICE"
}

# Fills the placeholder $1 of the config with a fresh X25519 private key in base64. A filled-in key is
# left alone, so redeploys keep the key. The key only passes through pipes and bash variables, never a
# file or a command line, and its public half is never stored: public_key_of derives it when needed.
fill_private_key() {
    local placeholder=$1 private_key config_text
    grep -qF "$placeholder" "$CONFIG_FILE" || return 0
    log "generating $placeholder"
    private_key=$(openssl genpkey -algorithm X25519 -outform DER | tail -c "$X25519_KEY_BYTES" | base64)
    [[ -n $private_key ]] || die "openssl did not produce a private key"
    config_text=$(<"$CONFIG_FILE")
    printf '%s\n' "${config_text//"$placeholder"/"$private_key"}" >"$CONFIG_FILE"
}

# The base64 public key of the base64 private key in field $1 of the config, as Noise derives it.
public_key_of() {
    local private_key
    private_key=$(sed -n "s/^$1 *= *\"\([^\"]*\)\".*/\1/p" "$CONFIG_FILE")
    { printf '%b' "$X25519_PRIVATE_KEY_DER_PREFIX"; printf '%s' "$private_key" | base64 -d; } \
        | openssl pkey -inform DER -pubout -outform DER | tail -c "$X25519_KEY_BYTES" | base64
}

# Runs the command $2 (install, uninstall or reinstall, optionally followed by --yes) with the
# script's own install_service and uninstall_service; $1 names the user to run as.
dispatch() {
    local user=$1 command=${2:-}
    [[ $EUID -ne 0 ]] || die "run as $user, not root"
    [[ ${3:-} == --yes ]] && ASSUME_YES=true
    case $command in
        install) install_service ;;
        uninstall) uninstall_service ;;
        reinstall) uninstall_service && install_service ;;
        *) die "usage: $0 install|uninstall|reinstall [--yes]" ;;
    esac
}
