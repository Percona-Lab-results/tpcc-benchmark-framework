#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="${SCRIPT_DIR}/bench.sh"
CLEANUP="${SCRIPT_DIR}/cleanup.sh"

DB="mysql"
BP=50
VU_START=1
VU_END=128
DURATION=3600
RAMPUP=60

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Sweep virtual users (doubling: 1,2,4,8,...) and run benchmarks.

Options:
  --db DB          mariadb, mysql, or percona (default: ${DB})
  --bp SIZE        Buffer pool in GB (default: ${BP})
  --start VU       Starting VU count (default: ${VU_START})
  --end VU         Max VU count (default: ${VU_END})
  --duration SECS  Benchmark duration (default: ${DURATION})
  --rampup SECS    Ramp-up time (default: ${RAMPUP})
  -h, --help       Show this help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)       DB="$2"; shift 2 ;;
        --bp)       BP="$2"; shift 2 ;;
        --start)    VU_START="$2"; shift 2 ;;
        --end)      VU_END="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --rampup)   RAMPUP="$2"; shift 2 ;;
        -h|--help)  usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Resolve DB-specific paths
case "$DB" in
    mariadb)
        CNF="${SCRIPT_DIR}/mariadb.cnf"
        START="${SCRIPT_DIR}/start.sh"
        CONTAINER="mariadb"
        ;;
    mysql)
        CNF="${SCRIPT_DIR}/mysql.cnf"
        START="${SCRIPT_DIR}/start_mysql.sh"
        CONTAINER="mysql"
        ;;
    percona)
        CNF="${SCRIPT_DIR}/percona.cnf"
        START="${SCRIPT_DIR}/start_percona.sh"
        CONTAINER="percona"
        ;;
    mysql97)
        CNF="${SCRIPT_DIR}/mysql97.cnf"
        START="${SCRIPT_DIR}/start_mysql97.sh"
        CONTAINER="mysql97"
        ;;
    *) die "Unknown DB: ${DB}. Use mariadb, mysql, percona, or mysql97." ;;
esac

[[ -f "$CNF" ]] || die "Config not found: ${CNF}"
[[ -f "$START" ]] || die "Start script not found: ${START}"

# Build VU list: 1, 2, 4, 8, 16, 32, 64, 128
VU_LIST=()
vu=$VU_START
while [[ $vu -le $VU_END ]]; do
    VU_LIST+=("$vu")
    vu=$(( vu * 2 ))
done

log "Sweep: DB=${DB} BP=${BP}G VUs=${VU_LIST[*]} duration=${DURATION}s"

# Save original config
cp "$CNF" "${CNF}.bak"
trap 'cat "${CNF}.bak" > "$CNF"; rm -f "${CNF}.bak"' EXIT

# Patch buffer pool size
instances=$(( BP / 5 ))
[[ "$instances" -lt 1 ]] && instances=1
sed "s/^innodb_buffer_pool_size.*/innodb_buffer_pool_size         = ${BP}G/" "$CNF" > "${CNF}.tmp"
if grep -q "^innodb_buffer_pool_instances" "${CNF}.tmp"; then
    sed -i "s/^innodb_buffer_pool_instances.*/innodb_buffer_pool_instances    = ${instances}/" "${CNF}.tmp"
fi
cat "${CNF}.tmp" > "$CNF"
rm -f "${CNF}.tmp"
log "Config: $(grep -E 'innodb_buffer_pool_(size|instances)' "$CNF")"

# Stop any running DB containers first
for c in $(docker ps -a --format '{{.Names}}' | grep -iE 'mysql|maria|percona'); do
    docker rm -f "$c" 2>/dev/null || true
done
sleep 2

for vu in "${VU_LIST[@]}"; do
    log "=========================================="
    log "Starting iteration: ${DB} BP=${BP}G VU=${vu}"
    log "=========================================="

    # Ensure no leftover container
    docker rm -f "$CONTAINER" 2>/dev/null || true
    sleep 2

    # Start DB
    log "Starting ${DB}..."
    bash "$START"

    # Wait for ready
    log "Waiting for ${DB} to be ready..."
    retries=60
    while ! mysql -h127.0.0.1 -P3306 -uroot -prootpassword -e "SELECT 1" &>/dev/null; do
        ((retries--)) || die "${DB} did not start for VU=${vu}"
        sleep 2
    done
    log "${DB} is ready."

    # Run benchmark
    log "Running benchmark: ${vu} VU, ${DURATION}s, BP=${BP}G"
    bash "$BENCH" -v "$vu" -d "$DURATION" -r "$RAMPUP" -l "${DB} BP ${BP}G VU ${vu}" || {
        log "Benchmark failed for VU=${vu}, continuing..."
    }

    # Cleanup
    log "Stopping ${DB}..."
    bash "$CLEANUP" "$CONTAINER" || true

    log "Iteration VU=${vu} complete."
    echo ""
done

log "=========================================="
log "Sweep complete: ${DB} BP=${BP}G VU ${VU_START}-${VU_END}"
log "=========================================="
