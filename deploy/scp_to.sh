#!/usr/bin/env bash
# Builds every release binary from scratch, copies the trading services to each host over scp, then
# reports which hosts received both and which did not. Tokyo is aarch64 and these binaries are x86_64,
# so Tokyo is never copied to: it counts as failed, and install_teyvat on Tokyo builds them there.
#
#   deploy/scp_to.sh <host>...      e.g. deploy/scp_to.sh Osaka Xian (hosts scp knows, such as ones in ~/.ssh/config)

set -euo pipefail

if (($# == 0)); then
    printf 'usage: %s <host>...\n' "$0" >&2
    exit 1
fi
readonly HOSTS=("$@")
# The aarch64 host; ssh matches host names case-insensitively, so this does too.
readonly ARM_HOST=Tokyo
# Each binary and the directory it goes to on every host.
readonly -A DESTINATIONS=(
    [market_data_provider]=/home/ubuntu/Services/MarketDataProvider/
    [dummy_strategy]=/home/ubuntu/Services/DummyStrategy/
)

succeeded_hosts=()
failed_hosts=()
copy_hosts=()
for host in "${HOSTS[@]}"; do
    if [[ ${host,,} == "${ARM_HOST,,}" ]]; then
        printf 'warning: %s is aarch64, so the x86_64 binaries built here cannot run there; skipping it.\n' "$host" >&2
        printf '         Run install_teyvat on %s to build it there.\n' "$host" >&2
        failed_hosts+=("$host (aarch64: run install_teyvat on $host)")
    else
        copy_hosts+=("$host")
    fi
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."
if ((${#copy_hosts[@]} > 0)); then
    rm -rf target
    cargo build --bins --examples --release
fi

for host in "${copy_hosts[@]}"; do
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
