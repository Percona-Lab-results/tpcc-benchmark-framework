#!/bin/bash
set -euo pipefail

# ─── Configuration ─────────────────────────────────────────────────────────────
HAMMERDB_VERSION="4.12"
HAMMERDB_DIR="/opt/hammerdb"

MARIADB_HOST="${DB_HOST:-127.0.0.1}"
MARIADB_PORT="${DB_PORT:-3306}"
MARIADB_USER="${DB_USER:-root}"
MARIADB_PASS="${DB_PASS:-rootpassword}"
MARIADB_DB="${DB_NAME:-tpcc}"

WAREHOUSES=1000
BUILD_VU=64
USE_PARTITION=true
USE_STORED_PROCS=true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Usage ────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Load TPC-C schema, data, and stored procedures.

Options:
  -w, --warehouses COUNT   Number of warehouses (default: ${WAREHOUSES})
  -v, --vu COUNT           Build virtual users (default: ${BUILD_VU})
  --no-partition           Disable table partitioning
  --no-stored-procs        Skip stored procedure creation
  -h, --help               Show this help

Environment:
  DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
EOF
    exit 0
}

# ─── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -w|--warehouses)    WAREHOUSES="$2"; shift 2 ;;
        -v|--vu)            BUILD_VU="$2"; shift 2 ;;
        --no-partition)     USE_PARTITION=false; shift ;;
        --no-stored-procs)  USE_STORED_PROCS=false; shift ;;
        -h|--help)          usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ─── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN:${NC} $*"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; exit 1; }

# ─── Wait for DB ──────────────────────────────────────────────────────────────
log "Waiting for database at ${MARIADB_HOST}:${MARIADB_PORT}..."
retries=120
while ! mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
              -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
              -e "SELECT 1" &>/dev/null; do
    ((retries--)) || die "Database did not become ready in time."
    sleep 2
done
log "Database is ready."

# ─── Create database ─────────────────────────────────────────────────────────
mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
      -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
      -e "CREATE DATABASE IF NOT EXISTS \`${MARIADB_DB}\`;" 2>/dev/null
log "Database '${MARIADB_DB}' ready."

# ─── Ensure libmariadb.so.3 ──────────────────────────────────────────────────
lib=$(find /usr/lib /usr/local/lib /lib -name "libmariadb.so.3" 2>/dev/null | head -1 || true)
[[ -n "$lib" ]] || die "libmariadb.so.3 not found. Install libmariadb3."
export LD_LIBRARY_PATH="$(dirname "$lib"):${LD_LIBRARY_PATH:-}"

# ─── Build schema via HammerDB ────────────────────────────────────────────────
log "Loading ${WAREHOUSES} warehouses with ${BUILD_VU} virtual users (partition=${USE_PARTITION})..."

cat > "${SCRIPT_DIR}/tpcc_build.tcl" <<TCL
puts "=== HammerDB TPC-C Schema Build ==="
puts "Warehouses : ${WAREHOUSES}"
puts "Build VUs  : ${BUILD_VU}"
puts "Partition  : ${USE_PARTITION}"

dbset db maria
dbset bm TPC-C

diset connection maria_host    ${MARIADB_HOST}
diset connection maria_port    ${MARIADB_PORT}
diset connection maria_socket  null
diset connection maria_ssl     false

diset tpcc maria_user          ${MARIADB_USER}
diset tpcc maria_pass          ${MARIADB_PASS}
diset tpcc maria_dbase         ${MARIADB_DB}
diset tpcc maria_storage_engine innodb
diset tpcc maria_count_ware    ${WAREHOUSES}
diset tpcc maria_num_vu        ${BUILD_VU}
diset tpcc maria_partition     ${USE_PARTITION}
diset tpcc maria_history_pk    false

buildschema
puts "=== Schema build complete ==="
TCL

(cd "${HAMMERDB_DIR}" && ./hammerdbcli auto "${SCRIPT_DIR}/tpcc_build.tcl") 2>&1 | tee "${SCRIPT_DIR}/build.log"

# ─── Verify tables ───────────────────────────────────────────────────────────
tables=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
               -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
               -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${MARIADB_DB}';" 2>/dev/null)
[[ "$tables" -ge 9 ]] || die "Schema build failed — only ${tables} tables found."
log "Schema OK: ${tables} tables."

# ─── Create stored procedures ─────────────────────────────────────────────────
if [[ "$USE_STORED_PROCS" == "true" ]]; then
    log "Creating stored procedures..."
    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          < "${SCRIPT_DIR}/create_procs.sql" 2>/dev/null

    procs=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                  -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                  -N -e "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema='${MARIADB_DB}' AND routine_type='PROCEDURE';" 2>/dev/null)
    log "Stored procedures: ${procs}"
else
    procs=0
    log "Skipping stored procedure creation (--no-stored-procs)"
fi

# ─── Done ─────────────────────────────────────────────────────────────────────
wh_count=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                 -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                 -N -e "SELECT COUNT(*) FROM ${MARIADB_DB}.warehouse;" 2>/dev/null)
log "=== Done. ${wh_count} warehouses loaded, ${procs} stored procedures created. ==="
