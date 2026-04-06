#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="${SCRIPT_DIR}/bench.sh"
CLEANUP="${SCRIPT_DIR}/cleanup.sh"

DB="mysql"
BP_START=10
BP_END=80
BP_STEP=10
VU=64
DURATION=3600
RAMPUP=60

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Sweep innodb_buffer_pool_size and run benchmarks.

Options:
  --db DB          mariadb, mysql, or percona (default: ${DB})
  --start SIZE     Starting BP in GB (default: ${BP_START})
  --end SIZE       Ending BP in GB (default: ${BP_END})
  --step SIZE      Step in GB (default: ${BP_STEP})
  --vu COUNT       Virtual users (default: ${VU})
  --duration SECS  Benchmark duration (default: ${DURATION})
  --rampup SECS    Ramp-up time (default: ${RAMPUP})
  -h, --help       Show this help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)       DB="$2"; shift 2 ;;
        --start)    BP_START="$2"; shift 2 ;;
        --end)      BP_END="$2"; shift 2 ;;
        --step)     BP_STEP="$2"; shift 2 ;;
        --vu)       VU="$2"; shift 2 ;;
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

log "Sweep: DB=${DB} BP=${BP_START}G-${BP_END}G step=${BP_STEP}G VU=${VU} duration=${DURATION}s"

# Save original config
cp "$CNF" "${CNF}.bak"
trap 'cat "${CNF}.bak" > "$CNF"; rm -f "${CNF}.bak"' EXIT

# Stop any running DB containers first
for c in $(docker ps -a --format '{{.Names}}' | grep -iE 'mysql|maria|percona'); do
    docker rm -f "$c" 2>/dev/null || true
done
sleep 2

for bp in $(seq "$BP_START" "$BP_STEP" "$BP_END"); do
    log "=========================================="
    log "Starting iteration: ${DB} innodb_buffer_pool_size = ${bp}G"
    log "=========================================="

    # Ensure no leftover container
    docker rm -f "$CONTAINER" 2>/dev/null || true
    sleep 2

    # Calculate buffer pool instances (each instance >= 5G, min 1)
    instances=$(( bp / 5 ))
    [[ "$instances" -lt 1 ]] && instances=1

    # Patch config (use cat > to preserve inode for docker bind mount)
    sed "s/^innodb_buffer_pool_size.*/innodb_buffer_pool_size         = ${bp}G/" "$CNF" > "${CNF}.tmp"
    # Update instances if the setting exists (MySQL/Percona only)
    if grep -q "^innodb_buffer_pool_instances" "${CNF}.tmp"; then
        sed -i "s/^innodb_buffer_pool_instances.*/innodb_buffer_pool_instances    = ${instances}/" "${CNF}.tmp"
    fi
    cat "${CNF}.tmp" > "$CNF"
    rm -f "${CNF}.tmp"
    log "Config patched: $(grep -E 'innodb_buffer_pool_(size|instances)' "$CNF")"

    # Start DB
    log "Starting ${DB}..."
    bash "$START"

    # Wait for ready
    log "Waiting for ${DB} to be ready..."
    retries=60
    while ! mysql -h127.0.0.1 -P3306 -uroot -prootpassword -e "SELECT 1" &>/dev/null; do
        ((retries--)) || die "${DB} did not start for BP=${bp}G"
        sleep 2
    done
    log "${DB} is ready."

    # Verify buffer pool matches expected
    actual_bp=$(mysql -h127.0.0.1 -P3306 -uroot -prootpassword -N -e "SELECT ROUND(@@innodb_buffer_pool_size / 1024/1024/1024);" 2>/dev/null)
    log "Verified buffer pool: ${actual_bp} GB (expected ${bp} GB)"
    if [[ "$actual_bp" != "$bp" ]]; then
        die "Buffer pool mismatch! Expected ${bp}G, got ${actual_bp}G. Check config mount."
    fi

    # Run benchmark
    log "Running benchmark: ${VU} VU, ${DURATION}s, BP=${bp}G"
    bash "$BENCH" -v "$VU" -d "$DURATION" -r "$RAMPUP" -l "${DB} BP ${bp}G sweep" || {
        log "Benchmark failed for BP=${bp}G, continuing..."
    }

    # Cleanup
    log "Stopping ${DB}..."
    bash "$CLEANUP" "$CONTAINER" || true

    log "Iteration BP=${bp}G complete."
    echo ""
done

log "=========================================="
log "Sweep complete: ${DB} BP ${BP_START}G to ${BP_END}G"
log "=========================================="
