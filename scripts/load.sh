#!/bin/sh
# Drive a write load on the primary with nothing but the mariadb client:
# N concurrent sessions, each CALLing a workload procedure from sql/labctl.sql
#
#   scripts/load.sh [clients] [tx_per_client] [workload]
#   scripts/load.sh 32 1000 independent      # the default
#   scripts/load.sh 32 1000 hotspot          # every tx also bumps one of 8 shared counters
#   scripts/load.sh 1 8000 independent       # one client: a serial primary
#
# Needs the lab schema: `python -m lab reset` (or any harness run) creates it
set -eu
CLIENTS=${1:-32}
TX=${2:-1000}
WL=${3:-independent}
ROWS=4
ACCOUNTS=200000
PW=${LAB_ROOT_PASSWORD:-lab}

case "$WL" in
  independent) call() { echo "CALL labctl.wl_independent($1, $CLIENTS, $TX, $ROWS, $ACCOUNTS)"; } ;;
  hotspot)     call() { echo "CALL labctl.wl_hotspot($1, $CLIENTS, $TX, $ROWS, $ACCOUNTS, 8)"; } ;;
  own_innodb|own_myisam)
               call() { echo "CALL labctl.wl_own_table($1, $TX, $ROWS, '${WL#own_}')"; } ;;
  *) echo "unknown workload $WL" >&2; exit 2 ;;
esac

echo "$(date +%T) starting $CLIENTS x $TX transactions ($WL)"
start=$(date +%s)
pids=""
c=0
while [ "$c" -lt "$CLIENTS" ]; do
  docker compose exec -T primary mariadb -uroot -p"$PW" -e "$(call $c)" &
  pids="$pids $!"
  c=$((c + 1))
done
failed=0
for pid in $pids; do
  wait "$pid" || failed=1
done
[ "$failed" -eq 0 ] || exit 1
end=$(date +%s)
echo "$(date +%T) done: $((CLIENTS * TX)) transactions in $((end - start)) s on the primary"
