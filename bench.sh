#!/bin/bash
set -euo pipefail

# ─── Configuration ─────────────────────────────────────────────────────────────
HAMMERDB_VERSION="4.12"
HAMMERDB_DIR="/opt/hammerdb"
HAMMERDB_URL="https://github.com/TPC-Council/HammerDB/releases/download/v${HAMMERDB_VERSION}/HammerDB-${HAMMERDB_VERSION}-Linux.tar.gz"

MARIADB_HOST="127.0.0.1"
MARIADB_PORT="3306"
MARIADB_USER="root"
MARIADB_PASS="rootpassword"
MARIADB_DB="tpcc"

WAREHOUSES=1000        # Must match the already-loaded schema
TEST_VU=64             # Virtual users for benchmark run
RAMPUP_SECONDS=120     # Warm-up time before measuring
DURATION_SECONDS=600   # Measurement window (default 10 min)
RUN_LABEL=""           # Optional label for the run

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_BASE="${SCRIPT_DIR}/results"

# ─── Usage ────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run TPC-C benchmark against an already-loaded MariaDB schema.
Each run stores results in its own directory under results/.

Options:
  -d, --duration SECONDS   Measurement duration (default: ${DURATION_SECONDS})
  -r, --rampup SECONDS     Ramp-up time before measuring (default: ${RAMPUP_SECONDS})
  -v, --vu COUNT           Number of virtual users (default: ${TEST_VU})
  -w, --warehouses COUNT   Warehouse count in schema (default: ${WAREHOUSES})
  -l, --label TEXT         Label for this run (added to directory name)
  -h, --help               Show this help
EOF
    exit 0
}

# ─── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--duration)   DURATION_SECONDS="$2"; shift 2 ;;
        -r|--rampup)     RAMPUP_SECONDS="$2"; shift 2 ;;
        -v|--vu)         TEST_VU="$2"; shift 2 ;;
        -w|--warehouses) WAREHOUSES="$2"; shift 2 ;;
        -l|--label)      RUN_LABEL="$2"; shift 2 ;;
        -h|--help)       usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Convert seconds to minutes for HammerDB (minimum 1 minute)
RAMPUP_MINUTES=$(( (RAMPUP_SECONDS + 59) / 60 ))
DURATION_MINUTES=$(( (DURATION_SECONDS + 59) / 60 ))

# ─── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN:${NC} $*"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $*" >&2; exit 1; }

# ─── Create run directory ─────────────────────────────────────────────────────
setup_run_dir() {
    local ts
    ts=$(date '+%Y%m%d_%H%M%S')
    local dirname="run_${ts}_vu${TEST_VU}_${DURATION_SECONDS}s"
    [[ -n "$RUN_LABEL" ]] && dirname="${dirname}_${RUN_LABEL}"
    RUN_DIR="${RESULTS_BASE}/${dirname}"
    mkdir -p "${RUN_DIR}"
    log "Run directory: ${RUN_DIR}"
}

# ─── Capture system & MariaDB config ──────────────────────────────────────────
capture_config() {
    log "Capturing configuration..."

    # Benchmark parameters
    cat > "${RUN_DIR}/bench_params.json" <<EOF
{
  "timestamp": "$(date -Iseconds)",
  "hammerdb_version": "${HAMMERDB_VERSION}",
  "mariadb_host": "${MARIADB_HOST}",
  "mariadb_port": "${MARIADB_PORT}",
  "database": "${MARIADB_DB}",
  "warehouses": ${WAREHOUSES},
  "virtual_users": ${TEST_VU},
  "rampup_seconds": ${RAMPUP_SECONDS},
  "duration_seconds": ${DURATION_SECONDS},
  "rampup_minutes": ${RAMPUP_MINUTES},
  "duration_minutes": ${DURATION_MINUTES},
  "label": "${RUN_LABEL}"
}
EOF

    # System info
    {
        echo "=== uname ==="
        uname -a
        echo ""
        echo "=== CPU ==="
        lscpu 2>/dev/null || cat /proc/cpuinfo | head -30
        echo ""
        echo "=== Memory ==="
        free -h
        echo ""
        echo "=== Disk ==="
        df -h /data 2>/dev/null || df -h /
        echo ""
        echo "=== Kernel parameters ==="
        sysctl vm.swappiness vm.dirty_ratio vm.dirty_background_ratio 2>/dev/null || true
    } > "${RUN_DIR}/system_info.txt"

    # MariaDB config file
    cp "${SCRIPT_DIR}/mariadb.cnf" "${RUN_DIR}/mariadb.cnf" 2>/dev/null || true

    # MariaDB variables
    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          -e "SHOW GLOBAL VARIABLES;" 2>/dev/null > "${RUN_DIR}/mariadb_variables.txt" || true

    # MariaDB status before run
    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          -e "SHOW GLOBAL STATUS;" 2>/dev/null > "${RUN_DIR}/mariadb_status_before.txt" || true

    # MariaDB version
    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          -N -e "SELECT VERSION();" 2>/dev/null > "${RUN_DIR}/mariadb_version.txt" || true
}

# ─── Background collectors ────────────────────────────────────────────────────
BG_PIDS=()

start_collectors() {
    local total_secs=$(( RAMPUP_SECONDS + DURATION_SECONDS + 60 ))

    # vmstat — 1 second intervals
    vmstat 1 "${total_secs}" > "${RUN_DIR}/vmstat.log" 2>&1 &
    BG_PIDS+=($!)
    log "Started vmstat collector (PID $!)"

    # iostat — 1 second intervals
    if command -v iostat &>/dev/null; then
        iostat -xdm 1 "${total_secs}" > "${RUN_DIR}/iostat.log" 2>&1 &
        BG_PIDS+=($!)
        log "Started iostat collector (PID $!)"
    fi

    # mpstat — per-CPU, 1 second intervals
    if command -v mpstat &>/dev/null; then
        mpstat -P ALL 1 "${total_secs}" > "${RUN_DIR}/mpstat.log" 2>&1 &
        BG_PIDS+=($!)
        log "Started mpstat collector (PID $!)"
    fi

    # MySQL QPS sampler — 1 second intervals
    _collect_mysql_qps &
    BG_PIDS+=($!)
    log "Started MySQL QPS collector (PID $!)"
}

_collect_mysql_qps() {
    local prev_questions="" prev_com_commit="" prev_com_rollback=""
    local prev_pages_flushed="" prev_purge_trx_id=""
    local outfile="${RUN_DIR}/qps.csv"
    echo "timestamp,questions,qps,com_commit,com_rollback,tps,threads_running,threads_connected,pages_flushed,pages_flushed_ps,purge_trx_id,purge_tps,history_list_length" > "$outfile"

    while true; do
        local stats
        stats=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                      -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                      -N -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Questions','Com_commit','Com_rollback','Threads_running','Threads_connected','Innodb_buffer_pool_pages_flushed','Innodb_purge_trx_id','Innodb_history_list_length');" 2>/dev/null) || { sleep 1; continue; }

        local questions com_commit com_rollback threads_running threads_connected
        local pages_flushed purge_trx_id history_list_length
        questions=$(echo "$stats" | awk '/^Questions/ {print $2}')
        com_commit=$(echo "$stats" | awk '/^Com_commit/ {print $2}')
        com_rollback=$(echo "$stats" | awk '/^Com_rollback/ {print $2}')
        threads_running=$(echo "$stats" | awk '/^Threads_running/ {print $2}')
        threads_connected=$(echo "$stats" | awk '/^Threads_connected/ {print $2}')
        pages_flushed=$(echo "$stats" | awk '/^Innodb_buffer_pool_pages_flushed/ {print $2}')
        purge_trx_id=$(echo "$stats" | awk '/^Innodb_purge_trx_id/ {print $2}')
        history_list_length=$(echo "$stats" | awk '/^Innodb_history_list_length/ {print $2}')

        # Defaults for MariaDB (no Innodb_purge_trx_id)
        pages_flushed=${pages_flushed:-0}
        purge_trx_id=${purge_trx_id:-0}
        history_list_length=${history_list_length:-0}

        local ts qps tps pages_flushed_ps purge_tps
        ts=$(date '+%Y-%m-%d %H:%M:%S')
        qps=0; tps=0; pages_flushed_ps=0; purge_tps=0

        if [[ -n "$prev_questions" ]]; then
            qps=$(( questions - prev_questions ))
            tps=$(( (com_commit - prev_com_commit) + (com_rollback - prev_com_rollback) ))
            pages_flushed_ps=$(( pages_flushed - prev_pages_flushed ))
            [[ "$prev_purge_trx_id" -gt 0 && "$purge_trx_id" -gt 0 ]] && purge_tps=$(( purge_trx_id - prev_purge_trx_id ))
        fi

        echo "${ts},${questions},${qps},${com_commit},${com_rollback},${tps},${threads_running},${threads_connected},${pages_flushed},${pages_flushed_ps},${purge_trx_id},${purge_tps},${history_list_length}" >> "$outfile"

        prev_questions=$questions
        prev_com_commit=$com_commit
        prev_com_rollback=$com_rollback
        prev_pages_flushed=$pages_flushed
        prev_purge_trx_id=$purge_trx_id

        sleep 1
    done
}

stop_collectors() {
    log "Stopping background collectors..."
    for pid in "${BG_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    BG_PIDS=()
}

# ─── Capture post-run status ──────────────────────────────────────────────────
capture_post_status() {
    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          -e "SHOW GLOBAL STATUS;" 2>/dev/null > "${RUN_DIR}/mariadb_status_after.txt" || true

    mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
          -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
          -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null > "${RUN_DIR}/innodb_status.txt" || true
}

# ─── Extract NOPM results from HammerDB output ───────────────────────────────
extract_results() {
    local logfile="${RUN_DIR}/hammerdb.log"

    # Extract NOPM_SAMPLE lines into CSV
    if grep -q "NOPM_SAMPLE:" "$logfile"; then
        echo "timestamp,nopm,tpm" > "${RUN_DIR}/nopm_samples.csv"
        grep "NOPM_SAMPLE:" "$logfile" | \
            awk '{print $2","$3","$4}' >> "${RUN_DIR}/nopm_samples.csv"
        local count
        count=$(grep -c "NOPM_SAMPLE:" "$logfile")
        log "Extracted ${count} NOPM samples to nopm_samples.csv"
    fi

    # Extract final NOPM/TPM result
    local result_line
    result_line=$(grep -E "System achieved|TEST RESULT" "$logfile" | tail -1 || true)
    if [[ -n "$result_line" ]]; then
        echo "$result_line" > "${RUN_DIR}/result.txt"
        log "Final result: ${result_line}"
    fi

    # Create summary
    {
        echo "=== Benchmark Run Summary ==="
        echo "Date:       $(date -Iseconds)"
        echo "Label:      ${RUN_LABEL:-<none>}"
        echo "Warehouses: ${WAREHOUSES}"
        echo "VUs:        ${TEST_VU}"
        echo "Ramp-up:    ${RAMPUP_SECONDS}s"
        echo "Duration:   ${DURATION_SECONDS}s"
        echo ""
        if [[ -n "$result_line" ]]; then
            echo "Result:     ${result_line}"
        fi
        echo ""
        if [[ -f "${RUN_DIR}/nopm_samples.csv" ]]; then
            echo "=== NOPM Statistics ==="
            # skip header, compute stats
            awk -F, 'NR>1 && $2>0 {
                n++; sum+=$2; if($2>max)max=$2; if(min==""||$2<min)min=$2;
                vals[n]=$2
            } END {
                if(n>0) {
                    avg=sum/n;
                    printf "  Samples:  %d\n", n
                    printf "  Avg NOPM: %.0f\n", avg
                    printf "  Min NOPM: %.0f\n", min
                    printf "  Max NOPM: %.0f\n", max
                }
            }' "${RUN_DIR}/nopm_samples.csv"
        fi
        echo ""
        if [[ -f "${RUN_DIR}/qps.csv" ]]; then
            echo "=== QPS Statistics ==="
            awk -F, 'NR>1 && $3>0 {
                n++; sum+=$3; if($3>max)max=$3; if(min==""||$3<min)min=$3
            } END {
                if(n>0) {
                    printf "  Samples: %d\n", n
                    printf "  Avg QPS: %.0f\n", sum/n
                    printf "  Min QPS: %.0f\n", min
                    printf "  Max QPS: %.0f\n", max
                }
            }' "${RUN_DIR}/qps.csv"
        fi
        echo ""
        echo "=== Files ==="
        ls -lh "${RUN_DIR}/" | awk 'NR>1 {printf "  %-35s %s\n", $NF, $5}'
    } > "${RUN_DIR}/summary.txt"

    cat "${RUN_DIR}/summary.txt"
}

# ─── Install HammerDB ──────────────────────────────────────────────────────────
install_hammerdb() {
    if [[ -x "${HAMMERDB_DIR}/hammerdbcli" ]]; then
        log "HammerDB already installed at ${HAMMERDB_DIR}"
        return
    fi

    log "Installing HammerDB ${HAMMERDB_VERSION}..."
    mkdir -p "${HAMMERDB_DIR}"

    TMP_TAR=$(mktemp /tmp/hammerdb-XXXXXX.tar.gz)
    curl -fsSL "${HAMMERDB_URL}" -o "${TMP_TAR}" || \
        die "Failed to download HammerDB from ${HAMMERDB_URL}"

    tar -xzf "${TMP_TAR}" -C "${HAMMERDB_DIR}" --strip-components=1
    rm -f "${TMP_TAR}"
    chmod +x "${HAMMERDB_DIR}/hammerdbcli"
    log "HammerDB installed at ${HAMMERDB_DIR}"
}

# ─── Ensure libmariadb.so.3 is available ──────────────────────────────────────
setup_mariadb_lib() {
    local lib
    lib=$(find /usr/lib /usr/local/lib /lib -name "libmariadb.so.3" 2>/dev/null | head -1 || true)

    if [[ -z "$lib" ]]; then
        log "libmariadb.so.3 not found — installing libmariadb3..."
        if command -v apt-get &>/dev/null; then
            apt-get install -y --no-install-recommends libmariadb3 2>/dev/null || \
            apt-get install -y --no-install-recommends libmariadb-dev 2>/dev/null || \
                die "Could not install libmariadb3. Run: apt-get install libmariadb3"
        elif command -v yum &>/dev/null; then
            yum install -y mariadb-connector-c || \
                die "Could not install mariadb-connector-c. Run: yum install mariadb-connector-c"
        elif command -v dnf &>/dev/null; then
            dnf install -y mariadb-connector-c || \
                die "Could not install mariadb-connector-c. Run: dnf install mariadb-connector-c"
        else
            die "libmariadb.so.3 not found and no supported package manager. Install libmariadb3 manually."
        fi
        lib=$(find /usr/lib /usr/local/lib /lib -name "libmariadb.so.3" 2>/dev/null | head -1 || true)
        [[ -n "$lib" ]] || die "libmariadb.so.3 still not found after install."
    fi

    export LD_LIBRARY_PATH="$(dirname "$lib"):${LD_LIBRARY_PATH:-}"
    log "Using MariaDB client library: $lib"
}

# ─── Wait for MariaDB to be ready ─────────────────────────────────────────────
wait_for_mariadb() {
    log "Waiting for MariaDB at ${MARIADB_HOST}:${MARIADB_PORT}..."
    local retries=30
    while ! mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                  -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                  -e "SELECT 1" &>/dev/null; do
        ((retries--)) || die "MariaDB did not become ready in time."
        sleep 2
    done
    log "MariaDB is ready."
}

# ─── Verify schema exists ─────────────────────────────────────────────────────
verify_schema() {
    log "Verifying TPC-C schema in database '${MARIADB_DB}'..."

    local tables
    tables=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                   -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                   -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${MARIADB_DB}';" 2>/dev/null)
    [[ "$tables" -ge 9 ]] || die "Database '${MARIADB_DB}' does not have the expected TPC-C tables (found ${tables}). Load data first."

    local procs
    procs=$(mysql -h"${MARIADB_HOST}" -P"${MARIADB_PORT}" \
                  -u"${MARIADB_USER}" -p"${MARIADB_PASS}" \
                  -N -e "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema='${MARIADB_DB}' AND routine_type='PROCEDURE';" 2>/dev/null)
    [[ "$procs" -ge 5 ]] || die "Database '${MARIADB_DB}' is missing stored procedures (found ${procs}, need 5). Create them first."

    log "Schema OK: ${tables} tables, ${procs} stored procedures."
}

# ─── Create TPC-C benchmark run Tcl script ────────────────────────────────────
write_tcl_script() {
    cat > "${RUN_DIR}/tpcc_run.tcl" <<TCL
puts "=== HammerDB TPC-C Benchmark Run ==="
puts "Virtual users : ${TEST_VU}"
puts "Ramp-up       : ${RAMPUP_MINUTES} min"
puts "Duration      : ${DURATION_MINUTES} min"
puts ""

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
diset tpcc maria_history_pk    false
diset tpcc maria_driver        timed
diset tpcc maria_rampup        ${RAMPUP_MINUTES}
diset tpcc maria_duration      ${DURATION_MINUTES}
diset tpcc maria_timeprofile   true
diset tpcc maria_allwarehouse  true

# 1-second polling loop
proc runtimer { total_secs } {
    set elapsed 0
    set prev_time [clock seconds]
    while { \$elapsed < \$total_secs } {
        after 1000
        set now [clock seconds]
        incr elapsed [expr { \$now - \$prev_time }]
        set prev_time \$now
        set mins [expr { \$elapsed / 60 }]
        set secs [expr { \$elapsed % 60 }]
        puts -nonewline "\r  Elapsed: [format %02d \$mins]:[format %02d \$secs] / [expr { \$total_secs / 60 }]:[format %02d [expr { \$total_secs % 60 }]]"
        flush stdout
        if { [vucomplete] } { break }
        update
    }
    puts ""
}

tcset refreshrate 1
loadscript

vuset vu ${TEST_VU}
vuset logtotemp 1
vucreate

set total_secs [expr { ${RAMPUP_SECONDS} + ${DURATION_SECONDS} + 30 }]
puts "Starting virtual users..."
vurun
runtimer \$total_secs

vudestroy
puts "=== Benchmark run complete ==="
TCL
}

# ─── Run HammerDB ─────────────────────────────────────────────────────────────
run_hammerdb() {
    local script="$1"
    local label="$2"
    log "Running HammerDB: ${label}"
    (cd "${HAMMERDB_DIR}" && LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" ./hammerdbcli auto "${script}") \
        2>&1 | tee "${RUN_DIR}/hammerdb.log"
}

# ─── Main ─────────────────────────────────────────────────────────────────────
main() {
    log "=== MariaDB TPC-C Benchmark ==="
    log "Warehouses : ${WAREHOUSES}"
    log "Test VUs   : ${TEST_VU}"
    log "Ramp-up    : ${RAMPUP_SECONDS}s (${RAMPUP_MINUTES} min)"
    log "Duration   : ${DURATION_SECONDS}s (${DURATION_MINUTES} min)"
    echo ""

    install_hammerdb
    setup_mariadb_lib
    wait_for_mariadb
    verify_schema

    setup_run_dir
    capture_config
    write_tcl_script

    # Ensure collectors stop on exit
    trap stop_collectors EXIT

    start_collectors

    run_hammerdb "${RUN_DIR}/tpcc_run.tcl" "TPC-C benchmark (${DURATION_SECONDS}s)"

    stop_collectors
    trap - EXIT

    capture_post_status
    extract_results

    echo ""
    log "=== Done. All data saved to: ${RUN_DIR} ==="
}

main "$@"
