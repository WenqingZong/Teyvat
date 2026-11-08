#!/usr/bin/env bash
# Builds every release binary from scratch, copies the trading services to each host over scp, then
# reports which hosts received both and which did not.
#
#   deploy/scp_to.sh <host>...      e.g. deploy/scp_to.sh Osaka Tokyo (hosts scp knows, such as ones in ~/.ssh/config)

set -euo pipefail

if (($# == 0)); then
    printf 'usage: %s <host>...\n' "$0" >&2
    exit 1
fi
readonly HOSTS=("$@")
# Each binary and the directory it goes to on every host.
readonly -A DESTINATIONS=(
    [market_data_provider]=/home/ubuntu/Services/MarketDataProvider/
    [dummy_strategy]=/home/ubuntu/Services/DummyStrategy/
)

cd "$(dirname "${BASH_SOURCE[0]}")/.."
rm -rf target
cargo build --bins --examples --release

succeeded_hosts=()
failed_hosts=()
for host in "${HOSTS[@]}"; do
    failed_binaries=()
    for binary in "${!DESTINATIONS[@]}"; do
        if ! scp "target/release/$binary" "$host:${DESTINATIONS[$binary]}"; then
            failed_binaries+=("$binary")
        fi
    done
    if ((${#failed_binaries[@]} == 0)); then
        succeeded_hosts+=("$host")
    else
        failed_hosts+=("$host (${failed_binaries[*]})")
    fi
done

printf '\nsucceeded: %d\n' "${#succeeded_hosts[@]}"
for host in "${succeeded_hosts[@]}"; do
    printf '    %s\n' "$host"
done
printf 'failed: %d\n' "${#failed_hosts[@]}"
for host in "${failed_hosts[@]}"; do
    printf '    %s\n' "$host"
done
((${#failed_hosts[@]} == 0))
