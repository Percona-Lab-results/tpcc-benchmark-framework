import os as _os
"""
Build static HTML report comparing MariaDB vs MySQL
from BP sweep and VU sweep benchmark runs.
"""
import json, base64, io, re, subprocess
from datetime import datetime
from collections import defaultdict, OrderedDict
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

# ── colours ──────────────────────────────────────────────────────────────────
C_MARIA  = "#f97316"   # MariaDB vivid orange
C_MYSQL  = "#06b6d4"   # MySQL cyan
C_BG     = "#080b14"   # Deep dark background
C_CARD   = "#0d1117"   # Chart / card area
C_GRID   = "#1c2438"   # Subtle grid lines
C_FG     = "#e4e8f0"   # Primary text
C_DIM    = "#64748b"   # Dimmed labels / ticks
C_AXIS   = "#2a3348"   # Spine / border colour

plt.rcParams.update({
    "figure.facecolor":  C_BG,
    "axes.facecolor":    C_CARD,
    "axes.edgecolor":    C_AXIS,
    "axes.labelcolor":   C_DIM,
    "text.color":        C_FG,
    "xtick.color":       C_DIM,
    "ytick.color":       C_DIM,
    "xtick.major.size":  0,
    "ytick.major.size":  0,
    "grid.color":        C_GRID,
    "grid.linewidth":    0.7,
    "legend.facecolor":  C_CARD,
    "legend.edgecolor":  C_AXIS,
    "legend.framealpha": 1.0,
    "legend.fontsize":   10,
    "font.family":       "DejaVu Sans",
    "font.size":         11,
    "axes.titlesize":    13,
    "axes.titlepad":     16,
    "axes.labelsize":    10,
})

# TPS (commits+rollbacks/s) → NOTPM (new orders/min): TPROC-C new-order mix = 45%
TPS_TO_NOTPM = 60 * 0.45

# ── load data ─────────────────────────────────────────────────────────────────
runs = json.load(open("data/runs.json"))

# Fix "unknown" MySQL 8.4.8 runs
for r in runs:
    if r["db"] == "unknown" and r.get("version", "").startswith("8.4."):
        r["db"] = "MySQL"

# ── helpers ───────────────────────────────────────────────────────────────────
def extract_bp_gb(label: str) -> int | None:
    m = re.search(r"(\d+)G", label)
    return int(m.group(1)) if m else None


ASSETS_DIR = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "report_assets")
_os.makedirs(ASSETS_DIR, exist_ok=True)

def fig_to_b64(fig, filename: str = None) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", pad_inches=0.28, facecolor=C_BG)
    if filename:
        path = _os.path.join(ASSETS_DIR, filename)
        with open(path, "wb") as f:
            f.write(buf.getvalue())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def qps_timeseries(run: dict):
    """Return (elapsed_min, notpm) arrays for steady-state portion."""
    rows = run["qps"]
    if not rows:
        return [], []
    t0 = datetime.fromisoformat(rows[0]["timestamp"])
    rampup = run.get("rampup_seconds", 60)
    elapsed, notpm = [], []
    for r in rows:
        t = datetime.fromisoformat(r["timestamp"])
        secs = (t - t0).total_seconds()
        if secs < rampup:
            continue
        v = float(r["tps"])
        if v > 0:
            elapsed.append(secs / 60)
            notpm.append(v * TPS_TO_NOTPM)
    return elapsed, notpm


def rolling_avg(values, window=30):
    result = []
    for i, v in enumerate(values):
        lo = max(0, i - window // 2)
        hi = min(len(values), i + window // 2 + 1)
        result.append(np.mean(values[lo:hi]))
    return result


def _clean_axes(ax):
    """Remove top/right spines, apply clean grid, kill tick marks."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(C_AXIS)
    ax.spines["bottom"].set_color(C_AXIS)
    ax.yaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.9)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.tick_params(axis="both", length=0, pad=6)


# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 1 — BP sweep  (64 VU, BP 10–80 G)
# ══════════════════════════════════════════════════════════════════════════════
bp_runs = [
    r for r in runs
    if "sweep" in r["label"].lower()
    and r["db"] in ("MariaDB", "MySQL")
    and r["virtual_users"] == 64
]

# Group by (db, bp_size) and average TPS
bp_data: dict[str, dict[int, list]] = defaultdict(lambda: defaultdict(list))
for r in bp_runs:
    size = extract_bp_gb(r["label"])
    if size and r["tps"].get("avg"):
        bp_data[r["db"]][size].append(r["tps"]["avg"])

def avg_bp(db) -> tuple[list, list]:
    sizes  = sorted(bp_data[db].keys())
    notpm  = [np.mean(bp_data[db][s]) * TPS_TO_NOTPM for s in sizes]
    return sizes, notpm

maria_bp_x, maria_bp_y = avg_bp("MariaDB")
mysql_bp_x,  mysql_bp_y  = avg_bp("MySQL")

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 2 — VU sweep  (BP 50G, VU 1–128)
# ══════════════════════════════════════════════════════════════════════════════
vu_runs = [
    r for r in runs
    if "VU" in r["label"]
    and "50G" in r["label"]
    and r["db"] in ("MariaDB", "MySQL")
    and "sweep" not in r["label"].lower()
]

def vu_series(db):
    pts = {}
    for r in vu_runs:
        if r["db"] == db and r["tps"].get("avg"):
            pts[r["virtual_users"]] = r["tps"]["avg"] * TPS_TO_NOTPM
    xs = sorted(pts)
    return xs, [pts[x] for x in xs]

maria_vu_x, maria_vu_y = vu_series("MariaDB")
mysql_vu_x,  mysql_vu_y  = vu_series("MySQL")

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 3 — TPS timeseries for representative runs
#  Pick: BP 50G, 64 VU run from VU sweep for each DB
# ══════════════════════════════════════════════════════════════════════════════
def best_run(db, label_fragment):
    cands = [
        r for r in runs
        if r["db"] == db and label_fragment.lower() in r["label"].lower()
        and r["tps"].get("avg", 0) > 0
    ]
    if not cands:
        return None
    return max(cands, key=lambda r: r["tps"]["avg"])

def best_vu_run(db, vu):
    cands = [
        r for r in vu_runs
        if r["db"] == db and r["virtual_users"] == vu and r["tps"].get("avg", 0) > 0
    ]
    return max(cands, key=lambda r: r["tps"]["avg"]) if cands else None

ts_maria = best_vu_run("MariaDB", 64)
ts_mysql  = best_vu_run("MySQL",   64)

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 4 — VU scaling efficiency (TPS / TPS@1VU)
# ══════════════════════════════════════════════════════════════════════════════
def scaling_eff(xs, ys):
    base = ys[0] if ys else 1
    return xs, [y / base for y in ys], base

maria_eff_x, maria_eff_y, maria_base = scaling_eff(maria_vu_x, maria_vu_y)
mysql_eff_x,  mysql_eff_y,  mysql_base  = scaling_eff(mysql_vu_x,  mysql_vu_y)
# linear reference: perfect scaling
eff_ref_x = [1, 128]
eff_ref_y  = [1, 128]


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 1 — BP sweep line chart
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_chart():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ys_m = [y/1000 for y in maria_bp_y]
    ys_q = [y/1000 for y in mysql_bp_y]

    ax.fill_between(maria_bp_x, ys_m, alpha=0.10, color=C_MARIA, lw=0)
    ax.fill_between(mysql_bp_x, ys_q, alpha=0.10, color=C_MYSQL, lw=0)
    ax.plot(maria_bp_x, ys_m, color=C_MARIA, lw=2.8,
            marker="o", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MARIA,
            markeredgewidth=2.2, label="MariaDB 12.2.2", zorder=5)
    ax.plot(mysql_bp_x, ys_q, color=C_MYSQL, lw=2.8,
            marker="s", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MYSQL,
            markeredgewidth=2.2, label="MySQL 8.4.8", zorder=5)

    ax.annotate(f"{maria_bp_y[-1]/1000:.1f}k", (maria_bp_x[-1], ys_m[-1]),
                textcoords="offset points", xytext=(10, 0), va="center",
                color=C_MARIA, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=C_MARIA+"55", lw=0.8))
    ax.annotate(f"{mysql_bp_y[-1]/1000:.1f}k", (mysql_bp_x[-1], ys_q[-1]),
                textcoords="offset points", xytext=(10, -10), va="center",
                color=C_MYSQL, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=C_MYSQL+"55", lw=0.8))

    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Buffer Pool Size  [64 VU · 3600 s]")
    ax.set_xticks(maria_bp_x)
    ax.set_xticklabels([f"{x}G" for x in maria_bp_x])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left")
    ax.set_xlim(8, 84)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig1_bp_line.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 2 — VU sweep line chart
# ══════════════════════════════════════════════════════════════════════════════
def make_vu_chart():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ys_m = [y/1000 for y in maria_vu_y]
    ys_q = [y/1000 for y in mysql_vu_y]

    ax.fill_between(maria_vu_x, ys_m, alpha=0.10, color=C_MARIA, lw=0)
    ax.fill_between(mysql_vu_x, ys_q, alpha=0.10, color=C_MYSQL, lw=0)
    ax.plot(maria_vu_x, ys_m, color=C_MARIA, lw=2.8,
            marker="o", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MARIA,
            markeredgewidth=2.2, label="MariaDB 12.2.2", zorder=5)
    ax.plot(mysql_vu_x, ys_q, color=C_MYSQL, lw=2.8,
            marker="s", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MYSQL,
            markeredgewidth=2.2, label="MySQL 8.4.8", zorder=5)

    ax.annotate(f"{maria_vu_y[-1]/1000:.1f}k", (maria_vu_x[-1], ys_m[-1]),
                textcoords="offset points", xytext=(-52, 8),
                color=C_MARIA, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=C_MARIA+"55", lw=0.8))
    ax.annotate(f"{mysql_vu_y[-1]/1000:.1f}k", (mysql_vu_x[-1], ys_q[-1]),
                textcoords="offset points", xytext=(8, 0), va="center",
                color=C_MYSQL, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=C_MYSQL+"55", lw=0.8))

    ax.set_xlabel("Virtual Users (log scale)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Concurrency  [BP 50G · 3600 s]")
    ax.set_xscale("log", base=2)
    ax.set_xticks(maria_vu_x)
    ax.set_xticklabels([str(x) for x in maria_vu_x])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left")
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig2_vu_line.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 3 — TPS time-series (BP 80G sweep run)
# ══════════════════════════════════════════════════════════════════════════════
def make_timeseries_chart():
    fig, ax = plt.subplots(figsize=(12, 5.0))

    for run, color, label in [
        (ts_maria, C_MARIA, "MariaDB 12.2.2"),
        (ts_mysql,  C_MYSQL,  "MySQL 8.4.8"),
    ]:
        if run is None:
            continue
        et, tps = qps_timeseries(run)
        if not et:
            continue
        smooth = rolling_avg(tps, window=60)
        ax.fill_between(et, [v/1000 for v in tps], alpha=0.07, color=color, lw=0)
        ax.plot(et, [v/1000 for v in tps],   color=color, lw=0.5, alpha=0.25)
        ax.plot(et, [v/1000 for v in smooth], color=color, lw=2.4, label=label)

    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel("NOTPM (thousands)")
    ax.set_title("NOTPM Over Time — Buffer Pool 50G  [64 VU · 3600 s]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend()
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig3_timeseries.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 4 — Scaling efficiency (VU sweep normalised to 1 VU)
# ══════════════════════════════════════════════════════════════════════════════
def make_scaling_chart():
    fig, ax = plt.subplots(figsize=(10, 5.5))

    ax.plot(eff_ref_x, eff_ref_y, color=C_AXIS, lw=1.5, ls="--",
            label="Linear (ideal)", alpha=0.5, zorder=1)
    ax.plot(maria_eff_x, maria_eff_y, color=C_MARIA, lw=2.8,
            marker="o", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MARIA,
            markeredgewidth=2.2, label="MariaDB 12.2.2", zorder=5)
    ax.plot(mysql_eff_x, mysql_eff_y, color=C_MYSQL, lw=2.8,
            marker="s", ms=8, markerfacecolor=C_BG, markeredgecolor=C_MYSQL,
            markeredgewidth=2.2, label="MySQL 8.4.8", zorder=5)

    ax.set_xlabel("Virtual Users (log scale)")
    ax.set_ylabel("NOTPM speedup vs 1 VU")
    ax.set_title("Concurrency Scaling Efficiency  [BP 50G · 3600 s]")
    ax.set_xscale("log", base=2)
    ax.set_xticks(maria_vu_x)
    ax.set_xticklabels([str(x) for x in maria_vu_x])
    ax.set_yscale("log", base=2)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}×"))
    ax.yaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.9)
    ax.xaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.6)
    ax.legend()
    _clean_axes(ax)
    # restore both grids overridden by _clean_axes
    ax.xaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.6)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig4_scaling.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 5 — BP sweep: grouped bar at each BP size
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_bar_chart():
    all_sizes = sorted(set(maria_bp_x) | set(mysql_bp_x))
    maria_vals = [np.mean(bp_data["MariaDB"].get(s, [0])) * TPS_TO_NOTPM / 1000 for s in all_sizes]
    mysql_vals  = [np.mean(bp_data["MySQL"].get(s,   [0])) * TPS_TO_NOTPM / 1000 for s in all_sizes]

    x = np.arange(len(all_sizes))
    w = 0.36

    fig, ax = plt.subplots(figsize=(12, 5.5))
    b1 = ax.bar(x - w/2, maria_vals, w, color=C_MARIA, label="MariaDB 12.2.2", lw=0, zorder=3)
    b2 = ax.bar(x + w/2, mysql_vals,  w, color=C_MYSQL,  label="MySQL 8.4.8",   lw=0, zorder=3)

    for bar in list(b1) + list(b2):
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x() + bar.get_width()/2, h + h * 0.012,
                    f"{h:.1f}k", ha="center", va="bottom", fontsize=8, color=C_FG, fontweight="600")

    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}G" for s in all_sizes])
    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput — Buffer Pool Sweep  [64 VU · 3600 s]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend()
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig5_bp_bar.png")


# ══════════════════════════════════════════════════════════════════════════════
#  Summary table data
# ══════════════════════════════════════════════════════════════════════════════
def pct_diff(a, b):
    if b == 0:
        return "—"
    d = (a - b) / b * 100
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.1f}%"

bp_table_rows = []
all_sizes = sorted(set(maria_bp_x) | set(mysql_bp_x))
maria_bp_map = dict(zip(maria_bp_x, maria_bp_y))
mysql_bp_map  = dict(zip(mysql_bp_x,  mysql_bp_y))
for s in all_sizes:
    m_notpm = maria_bp_map.get(s, 0)
    q_notpm = mysql_bp_map.get(s,  0)
    diff    = pct_diff(m_notpm, q_notpm)
    winner  = "MariaDB" if m_notpm > q_notpm else "MySQL"
    bp_table_rows.append((f"{s}G", int(m_notpm), int(q_notpm), diff, winner))

vu_table_rows = []
vu_pts_m = dict(zip(maria_vu_x, maria_vu_y))
vu_pts_q = dict(zip(mysql_vu_x,  mysql_vu_y))
all_vus = sorted(set(maria_vu_x) | set(mysql_vu_x))
for v in all_vus:
    m_tps = vu_pts_m.get(v, 0)
    q_tps = vu_pts_q.get(v, 0)
    diff  = pct_diff(m_tps, q_tps)
    winner = "MariaDB" if m_tps > q_tps else "MySQL"
    vu_table_rows.append((str(v), int(m_tps), int(q_tps), diff, winner))


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG EXTRACTION
#  Read mariadb.cnf from one representative run per DB (80G sweep)
# ══════════════════════════════════════════════════════════════════════════════
import os as _os
REPO = _os.path.dirname(_os.path.abspath(__file__))

# MariaDB-specific parameters that MySQL ignores
MARIA_ONLY = {
    "innodb_snapshot_isolation",
    "innodb_data_file_buffering",
    "innodb_data_file_write_through",
    "innodb_log_file_buffering",
    "innodb_log_file_write_through",
}

# Group headings for display
SECTION_MAP = OrderedDict([
    ("General",         {"user","datadir","socket","pid-file","bind-address","port",
                         "skip-name-resolve","performance_schema"}),
    ("Connections",     {"max_connections","max_connect_errors","thread_stack",
                         "thread_cache_size","back_log","wait_timeout",
                         "interactive_timeout","connect_timeout"}),
    ("InnoDB Buffer",   {"innodb_buffer_pool_size","innodb_buffer_pool_instances"}),
    ("InnoDB I/O",      {"innodb_io_capacity","innodb_io_capacity_max",
                         "innodb_read_io_threads","innodb_write_io_threads",
                         "innodb_use_native_aio",
                         "innodb_data_file_buffering","innodb_data_file_write_through",
                         "innodb_log_file_buffering","innodb_log_file_write_through"}),
    ("InnoDB Log",      {"innodb_log_file_size","innodb_log_buffer_size",
                         "innodb_flush_log_at_trx_commit","innodb_doublewrite"}),
    ("InnoDB OLTP",     {"innodb_snapshot_isolation","innodb_stats_on_metadata",
                         "innodb_open_files","innodb_lock_wait_timeout",
                         "innodb_rollback_on_timeout"}),
    ("Binary Log",      {"log_bin","binlog_format","binlog_row_image",
                         "expire_logs_days","sync_binlog","binlog_cache_size",
                         "max_binlog_size"}),
    ("Buffers",         {"sort_buffer_size","join_buffer_size","read_buffer_size",
                         "read_rnd_buffer_size","tmp_table_size","max_heap_table_size",
                         "bulk_insert_buffer_size"}),
    ("Cache / Misc",    {"query_cache_type","query_cache_size","table_open_cache",
                         "table_definition_cache","open_files_limit",
                         "max_allowed_packet","key_buffer_size",
                         "character_set_server","collation_server"}),
])


def read_cnf(run_dir: str) -> dict[str, str]:
    """Read mariadb.cnf from git and return active [mysqld] params."""
    result = subprocess.run(
        ["git", "show", f"HEAD:{run_dir}/mariadb.cnf"],
        cwd=REPO, capture_output=True, text=True,
    )
    params = {}
    in_section = False
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("[mysqld]"):
            in_section = True
            continue
        if line.startswith("[") and line != "[mysqld]":
            in_section = False
        if not in_section or not line or line.startswith("#"):
            continue
        # strip inline comments
        line = re.sub(r"\s*#.*$", "", line).strip()
        if "=" in line:
            k, v = line.split("=", 1)
            params[k.strip().lower()] = v.strip()
    return params


# Pick representative runs: best NOTPM run from BP 80G sweep for each DB
def rep_run_dir(db):
    cands = [
        r for r in runs
        if r["db"] == db and "BP 80G sweep" in r["label"]
    ]
    best = max(cands, key=lambda r: r["tps"].get("avg", 0))
    return "results/" + best["run_name"]

maria_cnf = read_cnf(rep_run_dir("MariaDB"))
mysql_cnf  = read_cnf(rep_run_dir("MySQL"))

# Build display rows: (section, param, maria_val, mysql_val)
def build_cfg_rows():
    seen = set()
    rows = []
    for section, keys in SECTION_MAP.items():
        section_rows = []
        for k in sorted(keys):
            m_val = maria_cnf.get(k, "")
            q_val = mysql_cnf.get(k, "")
            if not m_val and not q_val:
                continue
            seen.add(k)
            section_rows.append((section, k, m_val, q_val))
        rows.extend(section_rows)
    # catch anything not in SECTION_MAP
    rest = []
    for k in sorted(set(maria_cnf) | set(mysql_cnf)):
        if k not in seen:
            rest.append(("Other", k, maria_cnf.get(k, ""), mysql_cnf.get(k, "")))
    rows.extend(rest)
    return rows

cfg_rows = build_cfg_rows()


def cfg_html_rows():
    out = []
    cur_section = None
    for section, param, m_val, q_val in cfg_rows:
        if section != cur_section:
            cur_section = section
            out.append(f'<tr class="cfg-section"><td colspan="3">{section}</td></tr>')
        maria_only = param in MARIA_ONLY
        differs    = m_val != q_val and m_val and q_val
        p_cls = ' class="cfg-maria"' if maria_only else (' class="cfg-diff"' if differs else "")
        m_display = m_val or '<span class="cfg-na">n/a</span>'
        q_display = q_val or '<span class="cfg-na">n/a</span>'
        badge = ' <span class="badge-maria">MariaDB only</span>' if maria_only else ""
        out.append(
            f'<tr{p_cls}>'
            f'<td class="cfg-param">{param}{badge}</td>'
            f'<td>{m_display}</td>'
            f'<td>{q_display}</td>'
            f'</tr>'
        )
    return "\n".join(out)



# ══════════════════════════════════════════════════════════════════════════════
#  JITTER DATA  — last 30 minutes of each sweep run
# ══════════════════════════════════════════════════════════════════════════════
JITTER_WINDOW = 1800  # seconds


def last_n_notpm(run: dict, window_secs: int = JITTER_WINDOW) -> list:
    """Return NOTPM values for the last `window_secs` seconds of active data."""
    rows = run["qps"]
    if not rows:
        return []
    last_active = None
    for r in reversed(rows):
        if float(r.get("tps", 0)) > 0:
            last_active = datetime.fromisoformat(r["timestamp"])
            break
    if last_active is None:
        return []
    cutoff = last_active.timestamp() - window_secs
    values = []
    for r in rows:
        t = datetime.fromisoformat(r["timestamp"])
        v = float(r.get("tps", 0))
        if v > 0 and t.timestamp() >= cutoff:
            values.append(v * TPS_TO_NOTPM)
    return values


def jitter_stats(values: list) -> dict:
    if not values:
        return {}
    a = np.array(values)
    return {
        "mean": float(np.mean(a)),
        "std":  float(np.std(a)),
        "cv":   float(np.std(a) / np.mean(a) * 100),
        "p5":   float(np.percentile(a, 5)),
        "p95":  float(np.percentile(a, 95)),
    }


def _sweep_jitter(runs_list, key_fn):
    data = {"MariaDB": {}, "MySQL": {}}
    for r in runs_list:
        if r["db"] not in data:
            continue
        key = key_fn(r)
        if key is None:
            continue
        vals = last_n_notpm(r)
        if vals:
            data[r["db"]].setdefault(key, []).extend(vals)
    return data


bp_jitter = _sweep_jitter(bp_runs, lambda r: extract_bp_gb(r["label"]))
vu_jitter = _sweep_jitter(vu_runs, lambda r: r["virtual_users"])


def _boxplot_group(ax, keys, jitter_data, colors, w=0.32):
    offsets = {"MariaDB": -w / 2, "MySQL": w / 2}
    for db in ("MariaDB", "MySQL"):
        col = colors[db]
        for i, key in enumerate(keys):
            vals = jitter_data[db].get(key, [])
            if not vals:
                continue
            ax.boxplot(
                [v / 1000 for v in vals],
                positions=[i + offsets[db]],
                widths=w * 0.85,
                patch_artist=True,
                notch=False,
                showfliers=False,
                medianprops=dict(color="#ffffff", lw=2.2),
                boxprops=dict(facecolor=col + "28", alpha=1, linewidth=1.5, edgecolor=col),
                whiskerprops=dict(color=col, lw=1.4, alpha=0.8, linestyle=(0, (4, 3))),
                capprops=dict(color=col, lw=2.0),
                manage_ticks=False,
            )


# ── FIGURE 6 — Jitter box plots: BP sweep ────────────────────────────────────
def make_jitter_bp_chart():
    sizes = sorted(set(maria_bp_x) | set(mysql_bp_x))
    fig, ax = plt.subplots(figsize=(13, 5.8))
    _boxplot_group(ax, sizes, bp_jitter, {"MariaDB": C_MARIA, "MySQL": C_MYSQL})
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f"{s}G" for s in sizes])
    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("NOTPM (thousands) — last 30 min")
    ax.set_title("NOTPM Jitter — Buffer Pool Sweep  [64 VU · last 30 min of each run]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(handles=[mpatches.Patch(color=C_MARIA, label="MariaDB 12.2.2"),
                        mpatches.Patch(color=C_MYSQL,  label="MySQL 8.4.8")],
              loc="upper left")
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig6_jitter_bp.png")


# ── FIGURE 7 — Jitter box plots: VU sweep ────────────────────────────────────
def make_jitter_vu_chart():
    vus = sorted(set(maria_vu_x) | set(mysql_vu_x))
    fig, ax = plt.subplots(figsize=(13, 5.8))
    _boxplot_group(ax, vus, vu_jitter, {"MariaDB": C_MARIA, "MySQL": C_MYSQL})
    ax.set_xticks(range(len(vus)))
    ax.set_xticklabels([str(v) for v in vus])
    ax.set_xlabel("Virtual Users")
    ax.set_ylabel("NOTPM (thousands) — last 30 min")
    ax.set_title("NOTPM Jitter — VU Sweep  [BP 50G · last 30 min of each run]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(handles=[mpatches.Patch(color=C_MARIA, label="MariaDB 12.2.2"),
                        mpatches.Patch(color=C_MYSQL,  label="MySQL 8.4.8")],
              loc="upper left")
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig7_jitter_vu.png")


# ── Jitter table data ─────────────────────────────────────────────────────────
def _jitter_rows(jitter_data, key_label_fn):
    rows = []
    for key in sorted(set(k for db in jitter_data for k in jitter_data[db])):
        for db in ("MariaDB", "MySQL"):
            stats = jitter_stats(jitter_data[db].get(key, []))
            if stats:
                rows.append({"config": key_label_fn(key), "db": db, **stats})
    return rows

bp_jitter_rows = _jitter_rows(bp_jitter, lambda k: f"{k}G")
vu_jitter_rows = _jitter_rows(vu_jitter, str)


def _html_jitter_table(rows):
    out = [
        '<table class="data-table">',
        '<thead><tr>'
        '<th>Config</th><th>DB</th><th>Mean NOTPM</th>'
        '<th>Std Dev</th><th title="Coefficient of Variation = std_dev / mean × 100. Lower is more stable.">CV%</th><th>P5</th><th>P95</th><th>P5&#8209;P95 Range</th>'
        '</tr></thead><tbody>',
    ]
    for r in rows:
        clr = 'style="color:#f97316"' if r["db"] == "MariaDB" else 'style="color:#06b6d4"'
        out.append(
            f'<tr><td>{r["config"]}</td><td {clr}>{r["db"]}</td>'
            f'<td>{int(r["mean"]):,}</td><td>{int(r["std"]):,}</td>'
            f'<td>{r["cv"]:.1f}%</td><td>{int(r["p5"]):,}</td>'
            f'<td>{int(r["p95"]):,}</td><td>{int(r["p95"]-r["p5"]):,}</td></tr>'
        )
    out.append('</tbody></table>')
    return "\n".join(out)


def _md_jitter_table(rows):
    lines = [
        "| Config | DB | Mean NOTPM | Std Dev | CV% | P5 | P95 | P5-P95 Range |",
        "|--------|----|-----------|---------|-----|-----|-----|-------------|",
    ]
    for r in rows:
        lines.append(
            f'| {r["config"]} | {r["db"]} | {int(r["mean"]):,} | {int(r["std"]):,}'
            f' | {r["cv"]:.1f}% | {int(r["p5"]):,} | {int(r["p95"]):,}'
            f' | {int(r["p95"]-r["p5"]):,} |'
        )
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER HTML
# ══════════════════════════════════════════════════════════════════════════════
print("Generating charts...")
img_bp_line  = make_bp_chart()
img_bp_bar   = make_bp_bar_chart()
img_vu_line  = make_vu_chart()
img_ts       = make_timeseries_chart()
img_scaling    = make_scaling_chart()
img_jitter_bp  = make_jitter_bp_chart()
img_jitter_vu  = make_jitter_vu_chart()
print("Charts done.")

def table_row(cols, tag="td"):
    cells = "".join(f"<{tag}>{c}</{tag}>" for c in cols)
    return f"<tr>{cells}</tr>"

def winner_class(w, db):
    return ' class="win"' if w == db else ""


def bp_html_rows():
    rows = []
    for size, m, q, diff, winner in bp_table_rows:
        diff_cls = "pos" if winner == "MariaDB" else "neg"
        rows.append(
            f'<tr><td>{size}</td>'
            f'<td{winner_class(winner,"MariaDB")}>{m:,}</td>'
            f'<td{winner_class(winner,"MySQL")}>{q:,}</td>'
            f'<td class="{diff_cls}">{diff}</td></tr>'
        )
    return "\n".join(rows)


def vu_html_rows():
    rows = []
    for vu, m, q, diff, winner in vu_table_rows:
        diff_cls = "pos" if winner == "MariaDB" else "neg"
        rows.append(
            f'<tr><td>{vu}</td>'
            f'<td{winner_class(winner,"MariaDB")}>{m:,}</td>'
            f'<td{winner_class(winner,"MySQL")}>{q:,}</td>'
            f'<td class="{diff_cls}">{diff}</td></tr>'
        )
    return "\n".join(rows)


HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>MariaDB vs MySQL — TPROC-C Benchmark Report [Work In Progress]</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: #080b14;
    color: #cdd5e0;
    line-height: 1.65;
  }}
  a {{ color: #5b9bd5; }}

  /* ── layout ── */
  .page {{ max-width: 1120px; margin: 0 auto; padding: 48px 28px 96px; }}

  /* ── header ── */
  header {{
    border-bottom: 1px solid #1c2438;
    padding-bottom: 24px;
    margin-bottom: 52px;
  }}
  header h1 {{
    font-size: 1.75rem;
    font-weight: 700;
    color: #eef0f8;
    letter-spacing: -0.025em;
    line-height: 1.25;
  }}
  header .subtitle {{
    color: #4a5870;
    font-size: 0.875rem;
    margin-top: 8px;
    letter-spacing: 0.01em;
  }}
  .pills {{
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-top: 18px;
  }}
  .pill {{
    font-size: 0.74rem;
    padding: 3px 11px;
    border-radius: 20px;
    border: 1px solid #1c2438;
    color: #4a5870;
  }}
  .pill-maria {{ border-color: #f97316; color: #f97316; }}
  .pill-mysql  {{ border-color: #06b6d4; color: #06b6d4; }}

  /* ── sections ── */
  section {{ margin-bottom: 64px; }}
  section h2 {{
    font-size: 0.72rem;
    font-weight: 700;
    color: #3d5a78;
    text-transform: uppercase;
    letter-spacing: 0.13em;
    margin-bottom: 22px;
    padding-bottom: 10px;
    border-bottom: 1px solid #1c2438;
  }}
  section h3 {{
    font-size: 0.92rem;
    font-weight: 600;
    color: #9aa8be;
    margin: 34px 0 12px;
  }}
  p {{ color: #7a8898; font-size: 0.875rem; margin-bottom: 12px; max-width: 800px; line-height: 1.75; }}
  p strong {{ color: #cdd5e0; }}

  /* ── key metrics bar ── */
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }}
  .kpi {{
    background: #0d1117;
    border: 1px solid #1c2438;
    border-radius: 12px;
    padding: 18px 20px;
    transition: border-color 0.15s ease;
  }}
  .kpi:hover {{ border-color: #2a3f58; }}
  .kpi .kpi-label {{ font-size: 0.7rem; color: #3d5068; text-transform: uppercase; letter-spacing: 0.07em; font-weight: 500; }}
  .kpi .kpi-val   {{ font-size: 1.65rem; font-weight: 700; margin: 5px 0 2px; letter-spacing: -0.02em; }}
  .kpi .kpi-sub   {{ font-size: 0.7rem; color: #3d5068; margin-top: 2px; }}
  .kpi-val.maria  {{ color: #f97316; }}
  .kpi-val.mysql  {{ color: #06b6d4; }}
  .kpi-val.green  {{ color: #4ade80; }}

  /* ── chart ── */
  .chart {{ margin: 22px 0 8px; border-radius: 10px; overflow: hidden; border: 1px solid #1c2438; }}
  .chart img {{ width: 100%; display: block; }}
  .chart-caption {{
    font-size: 0.74rem;
    color: #364558;
    margin-top: 7px;
    margin-bottom: 28px;
    font-style: italic;
  }}

  /* ── two-up charts ── */
  .two-up {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  @media (max-width: 720px) {{ .two-up {{ grid-template-columns: 1fr; }} }}

  /* ── table ── */
  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.84rem;
    margin-top: 16px;
  }}
  .data-table th {{
    text-align: left;
    color: #3d5a78;
    padding: 10px 14px;
    border-bottom: 1px solid #1c2438;
    font-weight: 600;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
  }}
  .data-table td {{
    padding: 8px 14px;
    border-bottom: 1px solid #0d1117;
  }}
  .data-table tbody tr:nth-child(even) td {{ background: #090c14; }}
  .data-table tr:hover td {{ background: #101828 !important; }}
  .data-table td.win {{ font-weight: 700; color: #eef0f8; }}
  .data-table td.pos {{ color: #f97316; font-weight: 600; }}
  .data-table td.neg {{ color: #06b6d4; font-weight: 600; }}
  .data-table td:first-child {{ color: #4a5870; }}

  /* ── callout ── */
  .callout {{
    background: #0d1117;
    border-left: 3px solid #1c3a58;
    border-radius: 0 10px 10px 0;
    padding: 14px 20px;
    font-size: 0.875rem;
    color: #7a8898;
    margin: 24px 0;
    max-width: 820px;
  }}
  .callout strong {{ color: #cdd5e0; }}

  /* ── config table ── */
  .cfg-table {{ font-size: 0.82rem; }}
  .cfg-table .cfg-section td {{
    background: #060810;
    color: #3d5a78;
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.11em;
    padding: 12px 14px 5px;
    font-weight: 700;
    border-bottom: none;
  }}
  .cfg-table .cfg-param {{ font-family: 'Consolas','SF Mono',monospace; color: #a8b4c8; }}
  .cfg-table .cfg-maria td {{ background: #0f0b06; }}
  .cfg-table .cfg-maria .cfg-param {{ color: #f97316; }}
  .cfg-table .cfg-diff td {{ background: #0a0814; }}
  .cfg-table .cfg-diff .cfg-param {{ color: #a78bfa; }}
  .cfg-table td:nth-child(2), .cfg-table td:nth-child(3) {{
    font-family: 'Consolas','SF Mono',monospace;
    color: #667080;
  }}
  .cfg-na {{ color: #2a3348; font-style: italic; }}
  .badge-maria {{
    font-size: 0.6rem;
    background: #1a1006;
    border: 1px solid #f9731630;
    color: #f97316;
    padding: 1px 5px;
    border-radius: 3px;
    margin-left: 6px;
    vertical-align: middle;
    font-family: sans-serif;
    letter-spacing: 0;
  }}

  footer {{
    border-top: 1px solid #1c2438;
    padding-top: 24px;
    font-size: 0.74rem;
    color: #283040;
    text-align: center;
  }}
</style>
</head>
<body>
<div class="page">

<!-- ── HEADER ── -->
<header>
  <h1>MariaDB vs MySQL — TPROC-C Benchmark Report <span style="font-size:0.55em;font-weight:500;color:#f59e0b;background:#2d1f00;border:1px solid #f59e0b55;border-radius:6px;padding:3px 10px;vertical-align:middle;letter-spacing:0.04em;">[Work In Progress]</span></h1>
  <div class="subtitle">HammerDB 4.12 · TPROC-C · 1000 warehouses · Intel Xeon Gold 6230 (2×20c) · 187 GiB RAM · NVMe 2.9 TB</div>
  <div class="pills">
    <span class="pill pill-maria">MariaDB 12.2.2</span>
    <span class="pill pill-mysql">MySQL 8.4.8</span>
    <span class="pill">Ubuntu 24.04</span>
    <span class="pill">3600 s runs</span>
    <span class="pill">60 s ramp-up</span>
    <span class="pill">Generated {datetime.now().strftime("%Y-%m-%d")}</span>
  </div>
</header>

<!-- ── SECTION 1: EXECUTIVE SUMMARY ── -->
<section>
  <h2>Executive Summary</h2>

  <div class="kpi-grid">
    <div class="kpi">
      <div class="kpi-label">MariaDB peak NOTPM</div>
      <div class="kpi-val maria">{int(max(maria_bp_y)):,}</div>
      <div class="kpi-sub">BP 80G · 64 VU</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">MySQL peak NOTPM</div>
      <div class="kpi-val mysql">{int(max(mysql_bp_y)):,}</div>

      <div class="kpi-sub">BP 80G · 64 VU</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">MariaDB advantage @ 80G BP</div>
      <div class="kpi-val green">{(max(maria_bp_y)/max(mysql_bp_y)-1)*100:.0f}%</div>
      <div class="kpi-sub">higher throughput</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">MySQL advantage @ 128 VU</div>
      <div class="kpi-val mysql">{(mysql_vu_y[-1]/maria_vu_y[-1]-1)*100:.0f}%</div>
      <div class="kpi-sub">50G BP · 128 VU</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">MariaDB scaling (1→128 VU)</div>
      <div class="kpi-val maria">{maria_vu_y[-1]/maria_vu_y[0]:.0f}×</div>
      <div class="kpi-sub">@ BP 50G</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">MySQL scaling (1→128 VU)</div>
      <div class="kpi-val mysql">{mysql_vu_y[-1]/mysql_vu_y[0]:.0f}×</div>
      <div class="kpi-sub">@ BP 50G</div>
    </div>
  </div>

  <div class="callout">
    <strong>Key findings:</strong> MariaDB 12.2.2 outperforms MySQL 8.4.8 at all buffer pool sizes with 64 VU,
    delivering up to <strong>{(max(maria_bp_y)/max(mysql_bp_y)-1)*100:.0f}% more throughput</strong> at 80G BP.
    However, MySQL scales better under high concurrency — at 128 virtual users (50G BP)
    MySQL leads by <strong>{(mysql_vu_y[-1]/maria_vu_y[-1]-1)*100:.0f}%</strong>, suggesting
    more efficient lock/latch management at extreme thread counts.
    Both engines plateau between 64 and 128 VU, indicating CPU or InnoDB internal bottlenecks.
  </div>
</section>

<!-- ── SECTION 2: BUFFER POOL SWEEP ── -->
<section>
  <h2>Buffer Pool Sweep  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">64 VU · 10G – 80G</span></h2>

  <p>
    Both databases ran TPROC-C with 64 virtual users and a fixed buffer pool varying from 10 GiB to 80 GiB.
    The dataset represents 1000 warehouses (~100 GB working set), so an 80 GiB pool covers ~80% of hot data.
  </p>

  <div class="chart"><img src="data:image/png;base64,{img_bp_line}" alt="BP sweep line chart"></div>
  <div class="chart-caption">Figure 1 — Average NOTPM vs buffer pool size. Each point is the steady-state average (post-ramp-up). MariaDB 10G and MySQL 10G points are averaged across repeated runs.</div>

  <div class="chart"><img src="data:image/png;base64,{img_bp_bar}" alt="BP sweep bar chart"></div>
  <div class="chart-caption">Figure 2 — Side-by-side NOTPM comparison per buffer pool size. Values above bars are in thousands.</div>

  <h3>Buffer Pool Sweep — Data Table</h3>
  <table class="data-table">
    <thead>
      <tr>
        <th>BP Size</th>
        <th>MariaDB NOTPM</th>
        <th>MySQL NOTPM</th>
        <th>Δ (MariaDB vs MySQL)</th>
      </tr>
    </thead>
    <tbody>
      {bp_html_rows()}
    </tbody>
  </table>

  <div class="callout" style="margin-top:20px;">
    MariaDB leads at every buffer pool size.
    The gap is largest at <strong>70G</strong> ({pct_diff(np.mean(bp_data['MariaDB'].get(70,[0])), np.mean(bp_data['MySQL'].get(70,[0])))}) and <strong>80G</strong> ({pct_diff(np.mean(bp_data['MariaDB'].get(80,[0])), np.mean(bp_data['MySQL'].get(80,[0])))}),
    where the working set fits mostly in memory and I/O is minimised.
    At small pool sizes (10–30G) the gap narrows — both engines are I/O-bound and the difference is less pronounced.
  </div>
</section>

<!-- ── SECTION 3: VIRTUAL USERS SWEEP ── -->
<section>
  <h2>Virtual Users Sweep  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">BP 50G · 1 – 128 VU</span></h2>

  <p>
    Concurrency was swept from 1 to 128 virtual users with a fixed 50 GiB buffer pool.
    Each VU count ran for 3600 seconds with a 60-second ramp-up.
  </p>

  <div class="two-up">
    <div>
      <div class="chart"><img src="data:image/png;base64,{img_vu_line}" alt="VU sweep line chart"></div>
      <div class="chart-caption">Figure 3 — NOTPM vs virtual users (log₂ X-axis).</div>
    </div>
    <div>
      <div class="chart"><img src="data:image/png;base64,{img_scaling}" alt="Scaling efficiency"></div>
      <div class="chart-caption">Figure 4 — Speedup vs 1 VU on log/log axes. Dashed line = ideal linear scaling.</div>
    </div>
  </div>

  <h3>Virtual Users Sweep — Data Table</h3>
  <table class="data-table">
    <thead>
      <tr>
        <th>VU</th>
        <th>MariaDB NOTPM</th>
        <th>MySQL NOTPM</th>
        <th>Δ (MariaDB vs MySQL)</th>
      </tr>
    </thead>
    <tbody>
      {vu_html_rows()}
    </tbody>
  </table>

  <div class="callout" style="margin-top:20px;">
    MariaDB leads at low-to-medium concurrency (1–32 VU) at this buffer pool size.
    MySQL overtakes at <strong>64 VU</strong> and extends its lead at <strong>128 VU</strong> (+{(mysql_vu_y[-1]/maria_vu_y[-1]-1)*100:.0f}%).
    Both engines show diminishing returns beyond 64 VU — MariaDB essentially saturates between 64 and 128 VU,
    while MySQL continues to extract modest additional throughput.
  </div>
</section>

<!-- ── SECTION 4: NOTPM STABILITY ── -->
<section>
  <h2>NOTPM Stability  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">BP 50G · 64 VU · full run</span></h2>

  <p>
    The time-series below shows per-second NOTPM for the BP 50G, 64 VU run from each engine.
    Thin lines are raw 1-second samples; thick lines are 60-sample rolling averages.
  </p>

  <div class="chart"><img src="data:image/png;base64,{img_ts}" alt="NOTPM timeseries"></div>
  <div class="chart-caption">Figure 5 — NOTPM over elapsed time. BP 50G · 64 VU. Y-axis starts at zero. Ramp-up period excluded.</div>

  <div class="callout">
    MariaDB shows <strong>higher variance</strong> in raw per-second NOTPM, which is typical of its background flush
    behaviour. MySQL exhibits a flatter profile. Both engines maintain stable average throughput throughout the run,
    confirming results are representative steady-state performance rather than transient spikes.
  </div>
</section>


<!-- ── SECTION 5: JITTER ── -->
<section>
  <h2>NOTPM Jitter  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">last 30 min · box = P25&#8209;P75 · whiskers = P5&#8209;P95 · no outliers</span></h2>

  <p>
    Peak throughput tells only half the story. A database that delivers high average NOTPM but
    with frequent dips forces the application tier to absorb spikes through connection pooling,
    retry logic, or queuing — adding latency and complexity. In production OLTP systems, low
    jitter is often more operationally valuable than a marginally higher mean: it means
    predictable response times, simpler capacity planning, and fewer tail-latency violations
    under peak load.
  </p>
  <p>
    Each box shows the distribution of per-second NOTPM samples during the final
    30 minutes of a run. Box edges = P25/P75, centre line = median,
    whiskers = P5/P95. A narrow box means stable throughput; a tall box means high variance.
    The tables below include <strong>CV%</strong> (Coefficient of Variation = std&#8202;/&#8202;mean&#8202;&times;&#8202;100):
    a scale-free measure of relative variability — lower is more stable, and unlike raw std dev
    it is directly comparable across runs with different mean throughputs.
  </p>

  <h3>Buffer Pool Sweep</h3>
  <div class="chart"><img src="data:image/png;base64,{img_jitter_bp}" alt="BP jitter"></div>
  <div class="chart-caption">Figure 6 — NOTPM distribution per buffer pool size (last 30 min). Outliers excluded.</div>
  {_html_jitter_table(bp_jitter_rows)}

  <h3 style="margin-top:28px;">Virtual Users Sweep</h3>
  <div class="chart"><img src="data:image/png;base64,{img_jitter_vu}" alt="VU jitter"></div>
  <div class="chart-caption">Figure 7 — NOTPM distribution per VU count (last 30 min).</div>
  {_html_jitter_table(vu_jitter_rows)}
</section>

<!-- ── SECTION 6: CONFIGURATION ── -->
<section>
  <h2>Database Configuration</h2>
  <p>
    Both engines used the same base <code>my.cnf</code>, captured from each run's
    <code>mariadb.cnf</code> artifact. The only parameter that varies across runs is
    <code>innodb_buffer_pool_size</code> (set per sweep step).
    <span style="color:#f97316;font-weight:600;">MariaDB-only</span> parameters are
    highlighted — MySQL silently ignores them.
    Parameters that differ between the two are marked
    <span style="color:#a78bfa;font-weight:600;">purple</span>.
  </p>

  <table class="data-table cfg-table">
    <thead>
      <tr>
        <th>Parameter</th>
        <th style="color:#f97316;">MariaDB 12.2.2</th>
        <th style="color:#06b6d4;">MySQL 8.4.8</th>
      </tr>
    </thead>
    <tbody>
      {cfg_html_rows()}
    </tbody>
  </table>
</section>

<!-- ── SECTION 6: METHODOLOGY ── -->
<section>
  <h2>Methodology</h2>
  <p><strong>Benchmark:</strong> TPROC-C via HammerDB 4.12 (<code>tpcc_run.tcl</code>).</p>
  <p><strong>Workload:</strong> 1000 warehouses (~100 GB data), 60 s ramp-up, 3600 s measurement window.</p>
  <p><strong>Hardware:</strong> Intel Xeon Gold 6230 (2×20 cores, HT enabled = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB).</p>
  <p><strong>OS:</strong> Ubuntu 24.04, kernel 6.8.0-60-generic.</p>
  <p><strong>Metric:</strong> Average NOTPM (New Orders per Minute) derived from per-second commit rate x 60 x 0.45 (TPROC-C new-order mix). Computed over the steady-state window after ramp-up; multiple runs at the same configuration are averaged.</p>
  <p><strong>Buffer pool sweep:</strong> 64 VU, buffer pool varied 10–80 GiB in 10 GiB steps.</p>
  <p><strong>VU sweep:</strong> 50 GiB buffer pool, VU ∈ {{1, 2, 4, 8, 16, 32, 64, 128}}.</p>
</section>

<footer>
  Data source: <a href="https://github.com/Percona-Lab-results/tpcc-benchmark-framework">Percona-Lab-results/tpcc-benchmark-framework</a> ·
  Report generated {datetime.now().strftime("%Y-%m-%d %H:%M")}
</footer>

</div>
</body>
</html>
"""

out = "report.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"Report written -> {out}")


# ══════════════════════════════════════════════════════════════════════════════
#  MARKDOWN REPORT
# ══════════════════════════════════════════════════════════════════════════════
def md_bp_table():
    rows = ["| BP Size | MariaDB NOTPM | MySQL NOTPM | Delta |",
            "|---------|--------------|-------------|-------|"]
    for size, m, q, diff, winner in bp_table_rows:
        m_fmt = f"**{m:,}**" if winner == "MariaDB" else f"{m:,}"
        q_fmt = f"**{q:,}**" if winner == "MySQL"   else f"{q:,}"
        rows.append(f"| {size} | {m_fmt} | {q_fmt} | {diff} |")
    return "\n".join(rows)


def md_vu_table():
    rows = ["| VU | MariaDB NOTPM | MySQL NOTPM | Delta |",
            "|----|--------------|-------------|-------|"]
    for vu, m, q, diff, winner in vu_table_rows:
        m_fmt = f"**{m:,}**" if winner == "MariaDB" else f"{m:,}"
        q_fmt = f"**{q:,}**" if winner == "MySQL"   else f"{q:,}"
        rows.append(f"| {vu} | {m_fmt} | {q_fmt} | {diff} |")
    return "\n".join(rows)


def md_cfg_table():
    rows = ["| Parameter | MariaDB 12.2.2 | MySQL 8.4.8 | Note |",
            "|-----------|---------------|------------|------|"]
    cur_section = None
    for section, param, m_val, q_val in cfg_rows:
        if section != cur_section:
            cur_section = section
            rows.append(f"| **{section}** | | | |")
        note = "MariaDB only" if param in MARIA_ONLY else ("differs" if m_val != q_val and m_val and q_val else "")
        rows.append(f"| `{param}` | `{m_val or 'n/a'}` | `{q_val or 'n/a'}` | {note} |")
    return "\n".join(rows)


def build_md():
    peak_maria = int(max(maria_bp_y))
    peak_mysql  = int(max(mysql_bp_y))
    adv_bp   = (max(maria_bp_y)/max(mysql_bp_y)-1)*100
    adv_vu   = (mysql_vu_y[-1]/maria_vu_y[-1]-1)*100
    scale_m  = maria_vu_y[-1]/maria_vu_y[0]
    scale_q  = mysql_vu_y[-1]/mysql_vu_y[0]

    return f"""# [Work In Progress] MariaDB vs MySQL -- TPROC-C Benchmark Report

**HammerDB 4.12 | TPROC-C | 1000 warehouses | 3600 s runs | 60 s ramp-up**
**Hardware:** Intel Xeon Gold 6230 (2x20c, HT = 80 logical CPUs) | 187 GiB RAM | NVMe 2.9 TB
**OS:** Ubuntu 24.04 | kernel 6.8.0-60-generic | Generated: {datetime.now().strftime("%Y-%m-%d")}

---

## Executive Summary

| Metric | MariaDB 12.2.2 | MySQL 8.4.8 |
|--------|---------------|------------|
| Peak NOTPM (BP 80G, 64 VU) | **{peak_maria:,}** | {peak_mysql:,} |
| MariaDB advantage @ 80G BP | +{adv_bp:.0f}% | -- |
| Peak NOTPM (BP 50G, 128 VU) | {int(maria_vu_y[-1]):,} | **{int(mysql_vu_y[-1]):,}** |
| MySQL advantage @ 128 VU | -- | +{adv_vu:.0f}% |
| Scaling factor 1->128 VU (BP 50G) | {scale_m:.0f}x | {scale_q:.0f}x |

> **Key findings:** MariaDB 12.2.2 outperforms MySQL 8.4.8 at all buffer pool sizes with 64 VU,
> delivering up to **{adv_bp:.0f}% more throughput** at 80G BP.
> MySQL overtakes at high concurrency -- at 128 VU (50G BP) it leads by **{adv_vu:.0f}%**,
> suggesting more efficient lock/latch management at extreme thread counts.

---

## Buffer Pool Sweep -- 64 VU, 10G-80G

Both engines ran TPROC-C with 64 virtual users and buffer pool varied from 10 to 80 GiB.
The dataset is 1000 warehouses (~100 GB), so an 80 GiB pool covers ~80% of hot data.

![TPROC-C Throughput vs Buffer Pool Size](report_assets/fig1_bp_line.png)

![TPROC-C Throughput vs Buffer Pool Size -- bar chart](report_assets/fig5_bp_bar.png)

{md_bp_table()}

> MariaDB leads at every buffer pool size. The gap is largest at 70G and 80G where the working
> set fits mostly in memory. At small pool sizes (10-30G) both engines are I/O-bound and the
> difference narrows.

---

## Virtual Users Sweep -- BP 50G, 1-128 VU

Concurrency swept from 1 to 128 virtual users with a fixed 50 GiB buffer pool.

![TPROC-C Throughput vs Concurrency](report_assets/fig2_vu_line.png)

![Concurrency Scaling Efficiency](report_assets/fig4_scaling.png)

{md_vu_table()}

> MariaDB leads at 1-32 VU. MySQL overtakes at 64 VU and extends its lead at 128 VU (+{adv_vu:.0f}%).
> Both plateau between 64 and 128 VU -- MariaDB essentially saturates while MySQL extracts
> modest additional throughput, indicating better high-concurrency InnoDB internals.

---

## NOTPM Stability -- BP 50G, 64 VU

Per-second NOTPM for the BP 50G, 64 VU run from each engine (thick line = 60-sample rolling average).

![NOTPM Over Time](report_assets/fig3_timeseries.png)

> MariaDB shows higher variance in raw per-second NOTPM, typical of its background flush behaviour.
> MySQL exhibits a flatter profile. Both maintain stable average throughput throughout the run.

---

## Database Configuration

Both engines used the same base `my.cnf` -- only `innodb_buffer_pool_size` varies per sweep step.
Parameters marked *MariaDB only* are silently ignored by MySQL.

{md_cfg_table()}

---

## NOTPM Jitter — last 30 min of each run

Peak throughput tells only half the story. A database that delivers high average NOTPM but with
frequent dips forces the application tier to absorb spikes through connection pooling, retry logic,
or queuing — adding latency and complexity. In production OLTP systems, low jitter is often more
operationally valuable than a marginally higher mean: it means predictable response times, simpler
capacity planning, and fewer tail-latency violations under peak load.

Each box shows the distribution of per-second NOTPM samples during the final 30 minutes.
Box = P25–P75, centre = median, whiskers = P5–P95.
The tables include **CV%** (Coefficient of Variation = std / mean × 100): a scale-free measure of
relative variability. Lower is more stable; unlike raw std dev it is directly comparable across
runs with different mean throughputs.

### Buffer Pool Sweep

![NOTPM Jitter — BP Sweep](report_assets/fig6_jitter_bp.png)

{_md_jitter_table(bp_jitter_rows)}

### Virtual Users Sweep

![NOTPM Jitter — VU Sweep](report_assets/fig7_jitter_vu.png)

{_md_jitter_table(vu_jitter_rows)}

---

---

## Methodology

- **Benchmark:** TPROC-C via HammerDB 4.12 (`tpcc_run.tcl`)
- **Workload:** 1000 warehouses (~100 GB), 60 s ramp-up, 3600 s measurement window
- **Hardware:** Intel Xeon Gold 6230 (2x20 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB)
- **OS:** Ubuntu 24.04, kernel 6.8.0-60-generic
- **Metric:** NOTPM = per-second commit rate x 60 x 0.45 (TPROC-C new-order mix is 45%)
- **BP sweep:** 64 VU, buffer pool 10-80 GiB in 10 GiB steps; repeated runs at same size are averaged
- **VU sweep:** 50 GiB buffer pool, VU in {{1, 2, 4, 8, 16, 32, 64, 128}}

---

*Data source: [Percona-Lab-results/tpcc-benchmark-framework](https://github.com/Percona-Lab-results/tpcc-benchmark-framework)*
"""


md_out = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "REPORT.md")
with open(md_out, "w", encoding="utf-8") as f:
    f.write(build_md())
print(f"Markdown report written -> {md_out}")
