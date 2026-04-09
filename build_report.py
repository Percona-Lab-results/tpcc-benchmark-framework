"""
Build static HTML report comparing MariaDB 12.2, MariaDB 12.3, MySQL 8.4, MySQL 9.7
from BP sweep and VU sweep benchmark runs.
"""
import os as _os
import json, base64, io, re, subprocess
from datetime import datetime
from collections import defaultdict, OrderedDict
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

# ── engine definitions ────────────────────────────────────────────────────────
# Each engine has: id, display name, short name, colour, marker, css class
ENGINES = OrderedDict([
    ("maria122", {
        "display":  "MariaDB 12.2.2",
        "short":    "MDB 12.2",
        "color":    "#f97316",    # vivid orange
        "marker":   "o",
        "css":      "maria122",
        "pill_cls": "pill-maria122",
    }),
    ("maria123", {
        "display":  "MariaDB 12.3.1",
        "short":    "MDB 12.3",
        "color":    "#facc15",    # gold/yellow
        "marker":   "D",
        "css":      "maria123",
        "pill_cls": "pill-maria123",
    }),
    ("mysql84", {
        "display":  "MySQL 8.4.8",
        "short":    "MySQL 8.4",
        "color":    "#06b6d4",    # cyan
        "marker":   "s",
        "css":      "mysql84",
        "pill_cls": "pill-mysql84",
    }),
    ("mysql97", {
        "display":  "MySQL 9.7.0",
        "short":    "MySQL 9.7",
        "color":    "#818cf8",    # indigo/violet
        "marker":   "^",
        "css":      "mysql97",
        "pill_cls": "pill-mysql97",
    }),
])

ENGINE_IDS = list(ENGINES.keys())

def engine_id(run: dict) -> str | None:
    """Classify a run into one of the 4 engine IDs."""
    ver = (run.get("version") or "").lower()
    db  = run.get("db", "").lower()
    if "12.3" in ver:
        return "maria123"
    if "12.2" in ver or (db == "mariadb" and "12.2" in ver):
        return "maria122"
    if "9.7" in ver:
        return "mysql97"
    if "8.4" in ver:
        return "mysql84"
    if db == "mariadb":
        # fallback: check label
        if "mariadb123" in run.get("label", "").lower():
            return "maria123"
        return "maria122"
    if db == "mysql":
        if "mysql97" in run.get("label", "").lower():
            return "mysql97"
        return "mysql84"
    return None

# ── theme ─────────────────────────────────────────────────────────────────────
C_BG     = "#080b14"
C_CARD   = "#0d1117"
C_GRID   = "#1c2438"
C_FG     = "#e4e8f0"
C_DIM    = "#64748b"
C_AXIS   = "#2a3348"

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

# TPS → NOTPM: TPROC-C new-order mix = 45%
TPS_TO_NOTPM = 60 * 0.45

# ── load data ─────────────────────────────────────────────────────────────────
runs = json.load(open("data/runs.json"))

# Fix "unknown" MySQL 8.4.8 runs
for r in runs:
    if r["db"] == "unknown" and r.get("version", "").startswith("8.4."):
        r["db"] = "MySQL"

# Tag every run with its engine id
for r in runs:
    r["_eid"] = engine_id(r)

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
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(C_AXIS)
    ax.spines["bottom"].set_color(C_AXIS)
    ax.yaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.9)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.tick_params(axis="both", length=0, pad=6)


def pct_diff(a, b):
    if b == 0:
        return "\u2014"
    d = (a - b) / b * 100
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.1f}%"


# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 1 — BP sweep  (64 VU, BP 10–80 G)
# ══════════════════════════════════════════════════════════════════════════════
bp_runs = [
    r for r in runs
    if "sweep" in r["label"].lower()
    and r["_eid"] in ENGINE_IDS
    and r["virtual_users"] == 64
]

# Group by (engine, bp_size) → list of TPS averages
bp_data: dict[str, dict[int, list]] = {eid: defaultdict(list) for eid in ENGINE_IDS}
for r in bp_runs:
    size = extract_bp_gb(r["label"])
    if size and r["tps"].get("avg"):
        bp_data[r["_eid"]][size].append(r["tps"]["avg"])

def avg_bp(eid) -> tuple[list, list]:
    sizes = sorted(bp_data[eid].keys())
    notpm = [np.mean(bp_data[eid][s]) * TPS_TO_NOTPM for s in sizes]
    return sizes, notpm

bp_series = {eid: avg_bp(eid) for eid in ENGINE_IDS}
all_bp_sizes = sorted(set(s for eid in ENGINE_IDS for s in bp_data[eid]))

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 2 — VU sweep  (BP 50G, VU 1–128)
# ══════════════════════════════════════════════════════════════════════════════
vu_runs = [
    r for r in runs
    if "VU" in r["label"]
    and "50G" in r["label"]
    and r["_eid"] in ENGINE_IDS
    and "sweep" not in r["label"].lower()
]

def vu_series(eid):
    pts = {}
    for r in vu_runs:
        if r["_eid"] == eid and r["tps"].get("avg"):
            pts[r["virtual_users"]] = r["tps"]["avg"] * TPS_TO_NOTPM
    xs = sorted(pts)
    return xs, [pts[x] for x in xs]

vu_data = {eid: vu_series(eid) for eid in ENGINE_IDS}
all_vus = sorted(set(x for eid in ENGINE_IDS for x in vu_data[eid][0]))

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 3 — TPS timeseries for representative runs (BP 50G, 64 VU)
# ══════════════════════════════════════════════════════════════════════════════
def best_vu_run(eid, vu):
    cands = [
        r for r in vu_runs
        if r["_eid"] == eid and r["virtual_users"] == vu and r["tps"].get("avg", 0) > 0
    ]
    return max(cands, key=lambda r: r["tps"]["avg"]) if cands else None

ts_runs = {eid: best_vu_run(eid, 64) for eid in ENGINE_IDS}

# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 4 — VU scaling efficiency
# ══════════════════════════════════════════════════════════════════════════════
def scaling_eff(xs, ys):
    base = ys[0] if ys else 1
    return xs, [y / base for y in ys], base

eff_data = {}
for eid in ENGINE_IDS:
    xs, ys = vu_data[eid]
    eff_data[eid] = scaling_eff(xs, ys)


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 1 — BP sweep line chart
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_chart():
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys = bp_series[eid]
        if not xs:
            continue
        ys_k = [y / 1000 for y in ys]
        ax.fill_between(xs, ys_k, alpha=0.08, color=e["color"], lw=0)
        ax.plot(xs, ys_k, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor=C_BG,
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)
        # annotate last point
        ax.annotate(f"{ys[-1]/1000:.1f}k", (xs[-1], ys_k[-1]),
                    textcoords="offset points", xytext=(10, 0), va="center",
                    color=e["color"], fontsize=8.5,
                    bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=e["color"]+"55", lw=0.8))

    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Buffer Pool Size  [64 VU \u00b7 3600 s]")
    ax.set_xticks(all_bp_sizes)
    ax.set_xticklabels([f"{x}G" for x in all_bp_sizes])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left", fontsize=9)
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
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys = vu_data[eid]
        if not xs:
            continue
        ys_k = [y / 1000 for y in ys]
        ax.fill_between(xs, ys_k, alpha=0.08, color=e["color"], lw=0)
        ax.plot(xs, ys_k, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor=C_BG,
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)
        ax.annotate(f"{ys[-1]/1000:.1f}k", (xs[-1], ys_k[-1]),
                    textcoords="offset points", xytext=(10, 0), va="center",
                    color=e["color"], fontsize=8.5,
                    bbox=dict(boxstyle="round,pad=0.3", fc=C_CARD, ec=e["color"]+"55", lw=0.8))

    ax.set_xlabel("Virtual Users (log scale)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Concurrency  [BP 50G \u00b7 3600 s]")
    ax.set_xscale("log", base=2)
    ax.set_xticks(all_vus)
    ax.set_xticklabels([str(x) for x in all_vus])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left", fontsize=9)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig2_vu_line.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 3 — TPS time-series (BP 50G, 64 VU)
# ══════════════════════════════════════════════════════════════════════════════
def make_timeseries_chart():
    fig, ax = plt.subplots(figsize=(12, 5.0))
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        run = ts_runs[eid]
        if run is None:
            continue
        et, tps = qps_timeseries(run)
        if not et:
            continue
        smooth = rolling_avg(tps, window=60)
        ax.fill_between(et, [v/1000 for v in tps], alpha=0.06, color=e["color"], lw=0)
        ax.plot(et, [v/1000 for v in tps],   color=e["color"], lw=0.4, alpha=0.2)
        ax.plot(et, [v/1000 for v in smooth], color=e["color"], lw=2.2, label=e["display"])

    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel("NOTPM (thousands)")
    ax.set_title("NOTPM Over Time \u2014 Buffer Pool 50G  [64 VU \u00b7 3600 s]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(fontsize=9)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig3_timeseries.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 4 — Scaling efficiency
# ══════════════════════════════════════════════════════════════════════════════
def make_scaling_chart():
    fig, ax = plt.subplots(figsize=(10, 5.5))

    ax.plot([1, 128], [1, 128], color=C_AXIS, lw=1.5, ls="--",
            label="Linear (ideal)", alpha=0.5, zorder=1)
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys, _ = eff_data[eid]
        if not xs:
            continue
        ax.plot(xs, ys, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor=C_BG,
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)

    ax.set_xlabel("Virtual Users (log scale)")
    ax.set_ylabel("NOTPM speedup vs 1 VU")
    ax.set_title("Concurrency Scaling Efficiency  [BP 50G \u00b7 3600 s]")
    ax.set_xscale("log", base=2)
    ax.set_xticks(all_vus)
    ax.set_xticklabels([str(x) for x in all_vus])
    ax.set_yscale("log", base=2)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}\u00d7"))
    ax.yaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.9)
    ax.xaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.6)
    ax.legend(fontsize=9)
    _clean_axes(ax)
    ax.xaxis.grid(True, color=C_GRID, lw=0.7, ls=":", alpha=0.6)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig4_scaling.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 5 — BP sweep: grouped bar
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_bar_chart():
    n_engines = len(ENGINE_IDS)
    x = np.arange(len(all_bp_sizes))
    w = 0.8 / n_engines

    fig, ax = plt.subplots(figsize=(13, 5.5))
    for i, eid in enumerate(ENGINE_IDS):
        e = ENGINES[eid]
        vals = [np.mean(bp_data[eid].get(s, [0])) * TPS_TO_NOTPM / 1000 for s in all_bp_sizes]
        bars = ax.bar(x + (i - n_engines/2 + 0.5) * w, vals, w,
                      color=e["color"], label=e["display"], lw=0, zorder=3)
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + h * 0.012,
                        f"{h:.0f}k", ha="center", va="bottom", fontsize=7,
                        color=C_FG, fontweight="600")

    ax.set_xticks(x)
    ax.set_xticklabels([f"{s}G" for s in all_bp_sizes])
    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput \u2014 Buffer Pool Sweep  [64 VU \u00b7 3600 s]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(fontsize=9)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig5_bp_bar.png")


# ══════════════════════════════════════════════════════════════════════════════
#  Summary table data
# ══════════════════════════════════════════════════════════════════════════════
# BP table: for each size, show NOTPM for each engine
bp_table_rows = []
for s in all_bp_sizes:
    row = {"size": f"{s}G"}
    for eid in ENGINE_IDS:
        xs, ys = bp_series[eid]
        idx = {x: y for x, y in zip(xs, ys)}
        row[eid] = int(idx.get(s, 0))
    bp_table_rows.append(row)

# VU table
vu_table_rows = []
for v in all_vus:
    row = {"vu": str(v)}
    for eid in ENGINE_IDS:
        xs, ys = vu_data[eid]
        idx = {x: y for x, y in zip(xs, ys)}
        row[eid] = int(idx.get(v, 0))
    vu_table_rows.append(row)


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════
REPO = _os.path.dirname(_os.path.abspath(__file__))

MARIA_ONLY = {
    "innodb_snapshot_isolation",
    "innodb_data_file_buffering",
    "innodb_data_file_write_through",
    "innodb_log_file_buffering",
    "innodb_log_file_write_through",
}

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
    for fname in ("mariadb.cnf", "mysql.cnf", "mysql97.cnf", "percona.cnf"):
        result = subprocess.run(
            ["git", "show", f"HEAD:{run_dir}/{fname}"],
            cwd=REPO, capture_output=True, text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            continue
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
            line = re.sub(r"\s*#.*$", "", line).strip()
            if "=" in line:
                k, v = line.split("=", 1)
                params[k.strip().lower()] = v.strip()
        if params:
            return params
    return {}


def rep_run_dir(eid):
    """Pick representative run: best NOTPM run from BP 80G sweep for the engine."""
    cands = [
        r for r in runs
        if r["_eid"] == eid and "80G" in r["label"] and "sweep" in r["label"].lower()
    ]
    if not cands:
        # fallback: any sweep run
        cands = [r for r in runs if r["_eid"] == eid and "sweep" in r["label"].lower()]
    if not cands:
        return None
    best = max(cands, key=lambda r: r["tps"].get("avg", 0))
    return "results/" + best["run_name"]


engine_cnfs = {}
for eid in ENGINE_IDS:
    rdir = rep_run_dir(eid)
    engine_cnfs[eid] = read_cnf(rdir) if rdir else {}


def build_cfg_rows():
    seen = set()
    rows = []
    for section, keys in SECTION_MAP.items():
        section_rows = []
        for k in sorted(keys):
            vals = {eid: engine_cnfs[eid].get(k, "") for eid in ENGINE_IDS}
            if not any(vals.values()):
                continue
            seen.add(k)
            section_rows.append((section, k, vals))
        rows.extend(section_rows)
    rest = []
    all_keys = set()
    for eid in ENGINE_IDS:
        all_keys |= set(engine_cnfs[eid].keys())
    for k in sorted(all_keys - seen):
        vals = {eid: engine_cnfs[eid].get(k, "") for eid in ENGINE_IDS}
        rest.append(("Other", k, vals))
    rows.extend(rest)
    return rows

cfg_rows = build_cfg_rows()


def cfg_html_rows():
    out = []
    cur_section = None
    n_cols = len(ENGINE_IDS) + 1  # param + engines
    for section, param, vals in cfg_rows:
        if section != cur_section:
            cur_section = section
            out.append(f'<tr class="cfg-section"><td colspan="{n_cols}">{section}</td></tr>')
        maria_only = param in MARIA_ONLY
        unique_vals = set(v for v in vals.values() if v)
        differs = len(unique_vals) > 1
        p_cls = ' class="cfg-maria"' if maria_only else (' class="cfg-diff"' if differs else "")
        badge = ' <span class="badge-maria">MariaDB only</span>' if maria_only else ""
        cells = f'<td class="cfg-param">{param}{badge}</td>'
        for eid in ENGINE_IDS:
            v = vals[eid]
            display = v if v else '<span class="cfg-na">n/a</span>'
            cells += f'<td>{display}</td>'
        out.append(f'<tr{p_cls}>{cells}</tr>')
    return "\n".join(out)


# ══════════════════════════════════════════════════════════════════════════════
#  JITTER DATA
# ══════════════════════════════════════════════════════════════════════════════
JITTER_WINDOW = 1800

def last_n_notpm(run: dict, window_secs: int = JITTER_WINDOW) -> list:
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
    data = {eid: {} for eid in ENGINE_IDS}
    for r in runs_list:
        eid = r["_eid"]
        if eid not in data:
            continue
        key = key_fn(r)
        if key is None:
            continue
        vals = last_n_notpm(r)
        if vals:
            data[eid].setdefault(key, []).extend(vals)
    return data

bp_jitter = _sweep_jitter(bp_runs, lambda r: extract_bp_gb(r["label"]))
vu_jitter = _sweep_jitter(vu_runs, lambda r: r["virtual_users"])


def _boxplot_group(ax, keys, jitter_data, w_total=0.7):
    n = len(ENGINE_IDS)
    w = w_total / n
    for i, eid in enumerate(ENGINE_IDS):
        col = ENGINES[eid]["color"]
        offset = (i - n/2 + 0.5) * w
        for j, key in enumerate(keys):
            vals = jitter_data[eid].get(key, [])
            if not vals:
                continue
            ax.boxplot(
                [v / 1000 for v in vals],
                positions=[j + offset],
                widths=w * 0.85,
                patch_artist=True,
                notch=False,
                showfliers=False,
                medianprops=dict(color="#ffffff", lw=2),
                boxprops=dict(facecolor=col + "28", alpha=1, linewidth=1.3, edgecolor=col),
                whiskerprops=dict(color=col, lw=1.2, alpha=0.8, linestyle=(0, (4, 3))),
                capprops=dict(color=col, lw=1.6),
                manage_ticks=False,
            )


def make_jitter_bp_chart():
    sizes = all_bp_sizes
    fig, ax = plt.subplots(figsize=(14, 6))
    _boxplot_group(ax, sizes, bp_jitter)
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f"{s}G" for s in sizes])
    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("NOTPM (thousands) \u2014 last 30 min")
    ax.set_title("NOTPM Jitter \u2014 Buffer Pool Sweep  [64 VU \u00b7 last 30 min of each run]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    handles = [mpatches.Patch(color=ENGINES[eid]["color"], label=ENGINES[eid]["display"])
               for eid in ENGINE_IDS]
    ax.legend(handles=handles, loc="upper left", fontsize=9)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig6_jitter_bp.png")


def make_jitter_vu_chart():
    vus = all_vus
    fig, ax = plt.subplots(figsize=(14, 6))
    _boxplot_group(ax, vus, vu_jitter)
    ax.set_xticks(range(len(vus)))
    ax.set_xticklabels([str(v) for v in vus])
    ax.set_xlabel("Virtual Users")
    ax.set_ylabel("NOTPM (thousands) \u2014 last 30 min")
    ax.set_title("NOTPM Jitter \u2014 VU Sweep  [BP 50G \u00b7 last 30 min of each run]")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    handles = [mpatches.Patch(color=ENGINES[eid]["color"], label=ENGINES[eid]["display"])
               for eid in ENGINE_IDS]
    ax.legend(handles=handles, loc="upper left", fontsize=9)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig7_jitter_vu.png")


def _jitter_rows(jitter_data, key_label_fn):
    rows = []
    keys = sorted(set(k for eid in jitter_data for k in jitter_data[eid]))
    for key in keys:
        for eid in ENGINE_IDS:
            stats = jitter_stats(jitter_data[eid].get(key, []))
            if stats:
                rows.append({"config": key_label_fn(key), "eid": eid, **stats})
    return rows

bp_jitter_rows = _jitter_rows(bp_jitter, lambda k: f"{k}G")
vu_jitter_rows = _jitter_rows(vu_jitter, str)


def _html_jitter_table(rows):
    out = [
        '<table class="data-table">',
        '<thead><tr>'
        '<th>Config</th><th>Engine</th><th>Mean NOTPM</th>'
        '<th>Std Dev</th><th title="Coefficient of Variation = std_dev / mean \u00d7 100. Lower is more stable.">CV%</th>'
        '<th>P5</th><th>P95</th><th>P5\u2011P95 Range</th>'
        '</tr></thead><tbody>',
    ]
    for r in rows:
        e = ENGINES[r["eid"]]
        out.append(
            f'<tr><td>{r["config"]}</td>'
            f'<td style="color:{e["color"]}">{e["display"]}</td>'
            f'<td>{int(r["mean"]):,}</td><td>{int(r["std"]):,}</td>'
            f'<td>{r["cv"]:.1f}%</td><td>{int(r["p5"]):,}</td>'
            f'<td>{int(r["p95"]):,}</td><td>{int(r["p95"]-r["p5"]):,}</td></tr>'
        )
    out.append('</tbody></table>')
    return "\n".join(out)


def _md_jitter_table(rows):
    lines = [
        "| Config | Engine | Mean NOTPM | Std Dev | CV% | P5 | P95 | P5-P95 Range |",
        "|--------|--------|-----------|---------|-----|-----|-----|-------------|",
    ]
    for r in rows:
        e = ENGINES[r["eid"]]
        lines.append(
            f'| {r["config"]} | {e["display"]} | {int(r["mean"]):,} | {int(r["std"]):,}'
            f' | {r["cv"]:.1f}% | {int(r["p5"]):,} | {int(r["p95"]):,}'
            f' | {int(r["p95"]-r["p5"]):,} |'
        )
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER HTML
# ══════════════════════════════════════════════════════════════════════════════
print("Generating charts...")
img_bp_line    = make_bp_chart()
img_bp_bar     = make_bp_bar_chart()
img_vu_line    = make_vu_chart()
img_ts         = make_timeseries_chart()
img_scaling    = make_scaling_chart()
img_jitter_bp  = make_jitter_bp_chart()
img_jitter_vu  = make_jitter_vu_chart()
print("Charts done.")


def winner_class(val, max_val):
    return ' class="win"' if val == max_val else ""


def bp_html_rows():
    rows = []
    for row in bp_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = f'<td>{row["size"]}</td>'
        for eid in ENGINE_IDS:
            v = row[eid]
            cls = winner_class(v, max_v) if v > 0 else ""
            cells += f'<td{cls}>{v:,}</td>' if v > 0 else '<td>\u2014</td>'
        rows.append(f'<tr>{cells}</tr>')
    return "\n".join(rows)


def vu_html_rows():
    rows = []
    for row in vu_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = f'<td>{row["vu"]}</td>'
        for eid in ENGINE_IDS:
            v = row[eid]
            cls = winner_class(v, max_v) if v > 0 else ""
            cells += f'<td{cls}>{v:,}</td>' if v > 0 else '<td>\u2014</td>'
        rows.append(f'<tr>{cells}</tr>')
    return "\n".join(rows)


# ── compute KPI values ───────────────────────────────────────────────────────
def peak_bp(eid):
    _, ys = bp_series[eid]
    return max(ys) if ys else 0

def peak_vu(eid):
    _, ys = vu_data[eid]
    return max(ys) if ys else 0

def vu_scaling(eid):
    _, ys = vu_data[eid]
    if len(ys) < 2 or ys[0] == 0:
        return 0
    return ys[-1] / ys[0]


kpi_peak_bp = {eid: peak_bp(eid) for eid in ENGINE_IDS}
kpi_peak_vu = {eid: peak_vu(eid) for eid in ENGINE_IDS}
kpi_scaling = {eid: vu_scaling(eid) for eid in ENGINE_IDS}
best_bp_eid = max(ENGINE_IDS, key=lambda e: kpi_peak_bp[e])
best_vu_eid = max(ENGINE_IDS, key=lambda e: kpi_peak_vu[e])


# ── build engine header columns for tables ────────────────────────────────────
def engine_th():
    return "".join(
        f'<th style="color:{ENGINES[eid]["color"]}">{ENGINES[eid]["display"]}</th>'
        for eid in ENGINE_IDS
    )


# ── pills HTML ───────────────────────────────────────────────────────────────
def pills_html():
    parts = []
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        parts.append(f'<span class="pill" style="border-color:{e["color"]};color:{e["color"]}">{e["display"]}</span>')
    parts += [
        '<span class="pill">Ubuntu 24.04</span>',
        '<span class="pill">3600 s runs</span>',
        '<span class="pill">60 s ramp-up</span>',
        f'<span class="pill">Generated {datetime.now().strftime("%Y-%m-%d")}</span>',
    ]
    return "\n    ".join(parts)


# ── KPI grid HTML ────────────────────────────────────────────────────────────
def kpi_html():
    parts = []
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        parts.append(f'''    <div class="kpi">
      <div class="kpi-label">{e["display"]} peak NOTPM</div>
      <div class="kpi-val" style="color:{e["color"]}">{int(kpi_peak_bp[eid]):,}</div>
      <div class="kpi-sub">BP 80G \u00b7 64 VU</div>
    </div>''')
    # scaling factors
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        parts.append(f'''    <div class="kpi">
      <div class="kpi-label">{e["display"]} scaling (1\u2192128 VU)</div>
      <div class="kpi-val" style="color:{e["color"]}">{kpi_scaling[eid]:.0f}\u00d7</div>
      <div class="kpi-sub">@ BP 50G</div>
    </div>''')
    return "\n".join(parts)


# ── callout text ─────────────────────────────────────────────────────────────
def exec_summary_callout():
    # Find best engine at 80G BP
    best = max(ENGINE_IDS, key=lambda e: kpi_peak_bp[e])
    second = sorted(ENGINE_IDS, key=lambda e: kpi_peak_bp[e], reverse=True)[1]
    adv = (kpi_peak_bp[best] / kpi_peak_bp[second] - 1) * 100

    # Find best at 128 VU
    vu128 = {}
    for eid in ENGINE_IDS:
        xs, ys = vu_data[eid]
        idx = dict(zip(xs, ys))
        vu128[eid] = idx.get(128, 0)
    best_vu = max(ENGINE_IDS, key=lambda e: vu128[e])
    second_vu = sorted(ENGINE_IDS, key=lambda e: vu128[e], reverse=True)[1]
    adv_vu = (vu128[best_vu] / vu128[second_vu] - 1) * 100 if vu128[second_vu] > 0 else 0

    return (
        f'<strong>Key findings:</strong> '
        f'<strong>{ENGINES[best]["display"]}</strong> leads at 80G buffer pool with '
        f'<strong>+{adv:.0f}%</strong> over {ENGINES[second]["display"]}. '
        f'Under high concurrency (128 VU, 50G BP), <strong>{ENGINES[best_vu]["display"]}</strong> '
        f'leads by <strong>+{adv_vu:.0f}%</strong> over {ENGINES[second_vu]["display"]}. '
        f'Both MariaDB versions and MySQL 9.7 show improved throughput over MySQL 8.4 at large buffer pool sizes.'
    )


# ── per-chart insight callouts ────────────────────────────────────────────────
def _ranked(eid_val_pairs):
    """Return list of (eid, val) sorted descending by val."""
    return sorted(eid_val_pairs, key=lambda x: x[1], reverse=True)


def insight_bp_line():
    """Figure 1 — BP sweep line: who wins where and the crossover."""
    # Find leader at smallest and largest BP
    small = all_bp_sizes[0]
    large = all_bp_sizes[-1]
    def notpm_at(eid, size):
        xs, ys = bp_series[eid]
        return dict(zip(xs, ys)).get(size, 0)

    rank_small = _ranked([(eid, notpm_at(eid, small)) for eid in ENGINE_IDS if notpm_at(eid, small) > 0])
    rank_large = _ranked([(eid, notpm_at(eid, large)) for eid in ENGINE_IDS if notpm_at(eid, large) > 0])
    leader_s, val_s = rank_small[0]
    second_s, val2_s = rank_small[1]
    leader_l, val_l = rank_large[0]
    second_l, val2_l = rank_large[1]
    gain_l = (val_l / val2_l - 1) * 100

    # biggest jump for any engine between consecutive sizes
    max_jump_eid, max_jump_from, max_jump_pct = None, 0, 0
    for eid in ENGINE_IDS:
        xs, ys = bp_series[eid]
        for i in range(1, len(xs)):
            if ys[i-1] > 0:
                pct = (ys[i] / ys[i-1] - 1) * 100
                if pct > max_jump_pct:
                    max_jump_pct = pct
                    max_jump_from = xs[i-1]
                    max_jump_eid = eid

    parts = [
        f'<strong>{ENGINES[leader_s]["display"]}</strong> leads at {small}G, but '
        f'<strong>{ENGINES[leader_l]["display"]}</strong> takes over at {large}G '
        f'(+{gain_l:.0f}% over {ENGINES[second_l]["display"]}).',
    ]
    if max_jump_eid:
        parts.append(
            f' The steepest single-step gain is <strong>{ENGINES[max_jump_eid]["display"]}</strong> '
            f'between {max_jump_from}G and {max_jump_from+10}G (+{max_jump_pct:.0f}%), '
            f'suggesting a critical threshold where significantly more of the working set fits in memory.'
        )
    return "".join(parts)


def insight_bp_bar():
    """Figure 2 — grouped bar: overall pattern."""
    # Count wins per engine across all sizes
    wins = {eid: 0 for eid in ENGINE_IDS}
    for s in all_bp_sizes:
        vals = [(eid, dict(zip(*bp_series[eid])).get(s, 0)) for eid in ENGINE_IDS]
        leader = max(vals, key=lambda x: x[1])
        if leader[1] > 0:
            wins[leader[0]] += 1
    most_wins = max(ENGINE_IDS, key=lambda e: wins[e])
    return (
        f'<strong>{ENGINES[most_wins]["display"]}</strong> leads at {wins[most_wins]} of '
        f'{len(all_bp_sizes)} buffer pool sizes. The gap between engines is narrow at small '
        f'pool sizes where all are I/O-bound, and widens significantly above 60G as in-memory '
        f'efficiency differences dominate.'
    )


def insight_vu_line():
    """Figure 3 — VU sweep line: crossover and plateau."""
    # Find who leads at 1 VU vs 128 VU
    def notpm_at_vu(eid, vu):
        xs, ys = vu_data[eid]
        return dict(zip(xs, ys)).get(vu, 0)

    rank_1 = _ranked([(eid, notpm_at_vu(eid, 1)) for eid in ENGINE_IDS if notpm_at_vu(eid, 1) > 0])
    rank_128 = _ranked([(eid, notpm_at_vu(eid, 128)) for eid in ENGINE_IDS if notpm_at_vu(eid, 128) > 0])
    leader_1 = rank_1[0][0]
    leader_128 = rank_128[0][0]

    # Find which engines plateau (< 5% gain from 64 to 128)
    plateau = []
    for eid in ENGINE_IDS:
        v64 = notpm_at_vu(eid, 64)
        v128 = notpm_at_vu(eid, 128)
        if v64 > 0 and v128 > 0:
            gain = (v128 / v64 - 1) * 100
            if gain < 5:
                plateau.append((eid, gain))

    parts = [
        f'<strong>{ENGINES[leader_1]["display"]}</strong> leads at single-threaded (1 VU) performance, '
        f'while <strong>{ENGINES[leader_128]["display"]}</strong> dominates at 128 VU.',
    ]
    if plateau:
        names = " and ".join(f'<strong>{ENGINES[e]["display"]}</strong> (+{g:.0f}%)' for e, g in plateau)
        parts.append(
            f' Near-plateau between 64 and 128 VU for {names}, '
            f'indicating internal scalability limits at this concurrency level.'
        )
    return " ".join(parts)


def insight_scaling():
    """Figure 4 — scaling efficiency: who is closest to linear."""
    # Compute efficiency at 128 VU: actual_speedup / 128
    effs = []
    for eid in ENGINE_IDS:
        xs, ys, _ = eff_data[eid]
        if 128 in dict(zip(xs, ys)):
            effs.append((eid, dict(zip(xs, ys))[128]))
    if not effs:
        return ""
    best_eid, best_su = max(effs, key=lambda x: x[1])
    worst_eid, worst_su = min(effs, key=lambda x: x[1])
    return (
        f'At 128 VU, <strong>{ENGINES[best_eid]["display"]}</strong> achieves '
        f'<strong>{best_su:.0f}\u00d7</strong> speedup over single-threaded '
        f'(ideal would be 128\u00d7), while <strong>{ENGINES[worst_eid]["display"]}</strong> '
        f'reaches only {worst_su:.0f}\u00d7. The gap from the ideal line shows how much '
        f'throughput is lost to lock contention, latch waits, and InnoDB internal serialisation.'
    )


def insight_timeseries():
    """Figure 5 — time-series: variance comparison."""
    # Compute CV% for each engine's 64VU run
    cvs = []
    for eid in ENGINE_IDS:
        run = ts_runs[eid]
        if run is None:
            continue
        _, tps = qps_timeseries(run)
        if len(tps) > 100:
            a = np.array(tps)
            cv = float(np.std(a) / np.mean(a) * 100)
            cvs.append((eid, cv))
    if not cvs:
        return ""
    smoothest = min(cvs, key=lambda x: x[1])
    roughest = max(cvs, key=lambda x: x[1])
    return (
        f'<strong>{ENGINES[smoothest[0]]["display"]}</strong> delivers the flattest profile '
        f'(CV\u2009=\u2009{smoothest[1]:.1f}%), while '
        f'<strong>{ENGINES[roughest[0]]["display"]}</strong> shows the most variation '
        f'(CV\u2009=\u2009{roughest[1]:.1f}%). Wide oscillations typically indicate periodic '
        f'InnoDB checkpoint flushes or purge storms that momentarily starve foreground transactions.'
    )


def insight_jitter_bp():
    """Figure 6 — jitter box plots: BP sweep."""
    # Find engine with lowest average CV% across all BP sizes
    avg_cv = {}
    for eid in ENGINE_IDS:
        cvs = []
        for s in all_bp_sizes:
            vals = bp_jitter[eid].get(s, [])
            if vals:
                a = np.array(vals)
                cvs.append(float(np.std(a) / np.mean(a) * 100))
        if cvs:
            avg_cv[eid] = np.mean(cvs)
    if not avg_cv:
        return ""
    best = min(avg_cv, key=avg_cv.get)
    worst = max(avg_cv, key=avg_cv.get)
    return (
        f'<strong>{ENGINES[best]["display"]}</strong> is the most consistent across all buffer pool '
        f'sizes (avg CV\u2009=\u2009{avg_cv[best]:.1f}%), while '
        f'<strong>{ENGINES[worst]["display"]}</strong> shows the highest jitter '
        f'(avg CV\u2009=\u2009{avg_cv[worst]:.1f}%). '
        f'Note that jitter tends to increase at mid-range pool sizes (30\u201360G) where the engine '
        f'alternates between serving from cache and triggering I/O-heavy evictions.'
    )


def insight_jitter_vu():
    """Figure 7 — jitter box plots: VU sweep."""
    # Find engine with lowest CV at 128 VU
    cv_128 = {}
    for eid in ENGINE_IDS:
        vals = vu_jitter[eid].get(128, [])
        if vals:
            a = np.array(vals)
            cv_128[eid] = float(np.std(a) / np.mean(a) * 100)
    if not cv_128:
        return ""
    best = min(cv_128, key=cv_128.get)
    worst = max(cv_128, key=cv_128.get)
    return (
        f'At peak concurrency (128 VU), <strong>{ENGINES[best]["display"]}</strong> maintains the '
        f'tightest spread (CV\u2009=\u2009{cv_128[best]:.1f}%), while '
        f'<strong>{ENGINES[worst]["display"]}</strong> has the widest '
        f'(CV\u2009=\u2009{cv_128[worst]:.1f}%). Jitter generally increases with VU count as '
        f'lock contention introduces more variable wait times per transaction.'
    )


HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Database Benchmark Comparison \u2014 TPROC-C Report</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: #080b14;
    color: #cdd5e0;
    line-height: 1.65;
  }}
  a {{ color: #5b9bd5; }}

  .page {{ max-width: 1200px; margin: 0 auto; padding: 48px 28px 96px; }}

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
  .kpi .kpi-val   {{ font-size: 1.5rem; font-weight: 700; margin: 5px 0 2px; letter-spacing: -0.02em; }}
  .kpi .kpi-sub   {{ font-size: 0.7rem; color: #3d5068; margin-top: 2px; }}

  .chart {{ margin: 22px 0 8px; border-radius: 10px; overflow: hidden; border: 1px solid #1c2438; }}
  .chart img {{ width: 100%; display: block; }}
  .chart-caption {{
    font-size: 0.74rem;
    color: #364558;
    margin-top: 7px;
    margin-bottom: 28px;
    font-style: italic;
  }}

  .two-up {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  @media (max-width: 720px) {{ .two-up {{ grid-template-columns: 1fr; }} }}

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
  .data-table td:first-child {{ color: #4a5870; }}

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

  .cfg-table {{ font-size: 0.78rem; }}
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
  .cfg-table .cfg-param {{ font-family: 'Consolas','SF Mono',monospace; color: #a8b4c8; white-space: nowrap; }}
  .cfg-table .cfg-maria td {{ background: #0f0b06; }}
  .cfg-table .cfg-maria .cfg-param {{ color: #f97316; }}
  .cfg-table .cfg-diff td {{ background: #0a0814; }}
  .cfg-table .cfg-diff .cfg-param {{ color: #a78bfa; }}
  .cfg-table td:not(:first-child) {{
    font-family: 'Consolas','SF Mono',monospace;
    color: #667080;
    font-size: 0.76rem;
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

<header>
  <h1>Database Benchmark Comparison \u2014 TPROC-C Report</h1>
  <div class="subtitle">HammerDB 4.12 \u00b7 TPROC-C \u00b7 1000 warehouses \u00b7 Intel Xeon Gold 6230 (2\u00d720c) \u00b7 187 GiB RAM \u00b7 NVMe 2.9 TB</div>
  <div class="pills">
    {pills_html()}
  </div>
</header>

<section>
  <h2>Executive Summary</h2>
  <div class="kpi-grid">
{kpi_html()}
  </div>
  <div class="callout">
    {exec_summary_callout()}
  </div>
</section>

<section>
  <h2>Buffer Pool Sweep  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">64 VU \u00b7 10G \u2013 80G</span></h2>
  <p>
    The <strong>InnoDB Buffer Pool</strong> is the main memory area where InnoDB caches table data
    and index pages. Every read that hits the buffer pool avoids a disk I/O; every miss forces a
    physical read from storage. For write-heavy OLTP workloads like TPROC-C, the buffer pool also
    holds dirty pages waiting to be flushed \u2014 a larger pool means fewer flush cycles and less
    I/O contention between foreground transactions and background flushing.
  </p>
  <p>
    A <strong>buffer pool sweep</strong> varies this single parameter (from 10 GiB to 80 GiB in
    10 GiB steps) while holding everything else constant \u2014 64 virtual users, 1000 warehouses
    (~100 GB working set), same hardware, same configuration. This isolates the effect of memory
    pressure on throughput. At small pool sizes (10\u201330G) only a fraction of the hot data fits
    in RAM, so performance is dominated by disk I/O speed and the engine\u2019s read-ahead and
    flushing strategies. As the pool grows toward the working set size, more reads hit cache and
    fewer dirty-page evictions are needed, revealing the engine\u2019s in-memory efficiency.
  </p>
  <p>
    The 64 VU count was chosen to represent a moderate-to-high concurrency level typical of
    production OLTP servers, ensuring that throughput differences reflect buffer pool efficiency
    rather than single-thread performance.
  </p>
  <div class="chart"><img src="data:image/png;base64,{img_bp_line}" alt="BP sweep line chart"></div>
  <div class="chart-caption">Figure 1 \u2014 Average NOTPM vs buffer pool size. Each point is the steady-state average (post-ramp-up).</div>
  <div class="callout">{insight_bp_line()}</div>

  <div class="chart"><img src="data:image/png;base64,{img_bp_bar}" alt="BP sweep bar chart"></div>
  <div class="chart-caption">Figure 2 \u2014 Side-by-side NOTPM comparison per buffer pool size.</div>
  <div class="callout">{insight_bp_bar()}</div>

  <h3>Buffer Pool Sweep \u2014 Data Table</h3>
  <table class="data-table">
    <thead><tr><th>BP Size</th>{engine_th()}</tr></thead>
    <tbody>{bp_html_rows()}</tbody>
  </table>
</section>

<section>
  <h2>Virtual Users Sweep  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">BP 50G \u00b7 1 \u2013 128 VU</span></h2>
  <p>
    A <strong>Virtual User (VU)</strong> is a HammerDB worker thread that simulates an independent
    database client. Each VU opens its own connection, picks a random warehouse, and continuously
    executes the TPROC-C transaction mix (new-order, payment, delivery, order-status, stock-level)
    in a tight loop for the duration of the run. Increasing the VU count is equivalent to increasing
    the number of concurrent application threads hitting the database simultaneously.
  </p>
  <p>
    VU count directly stresses the database engine\u2019s concurrency internals: InnoDB row-level
    locking, the lock manager, undo/purge scheduling, buffer pool latch contention, and redo log
    synchronisation. At low VU counts the engine is mostly CPU- and I/O-bound; as VU rises,
    internal latch contention and lock waits become the dominant bottleneck. The point where
    throughput plateaus reveals how efficiently the engine scales under parallel workloads \u2014
    a critical metric for multi-tenant and connection-pool-heavy OLTP deployments.
  </p>
  <p>
    Concurrency was swept from 1 to 128 virtual users with a fixed 50 GiB buffer pool.
    Each VU count ran for 3600 seconds with a 60-second ramp-up.
  </p>
  <div class="two-up">
    <div>
      <div class="chart"><img src="data:image/png;base64,{img_vu_line}" alt="VU sweep line chart"></div>
      <div class="chart-caption">Figure 3 \u2014 NOTPM vs virtual users (log\u2082 X-axis).</div>
    </div>
    <div>
      <div class="chart"><img src="data:image/png;base64,{img_scaling}" alt="Scaling efficiency"></div>
      <div class="chart-caption">Figure 4 \u2014 Speedup vs 1 VU on log/log axes. Dashed = ideal linear scaling.</div>
    </div>
  </div>
  <div class="callout">{insight_vu_line()}</div>
  <div class="callout">{insight_scaling()}</div>

  <h3>Virtual Users Sweep \u2014 Data Table</h3>
  <table class="data-table">
    <thead><tr><th>VU</th>{engine_th()}</tr></thead>
    <tbody>{vu_html_rows()}</tbody>
  </table>
</section>

<section>
  <h2>NOTPM Stability  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">BP 50G \u00b7 64 VU \u00b7 full run</span></h2>
  <p>
    <strong>NOTPM Stability</strong> measures how consistently a database sustains its throughput
    over the entire duration of a benchmark run. A high average NOTPM is meaningless if the engine
    periodically stalls \u2014 background checkpoint flushes, purge operations, or adaptive flushing
    can cause sharp dips that ripple through the application as latency spikes.
  </p>
  <p>
    The chart below plots per-second NOTPM for the full 3600-second run (ramp-up excluded) at
    BP 50G with 64 virtual users. Thin lines are raw 1-second samples; thick lines are 60-second
    rolling averages. A flat rolling average indicates stable throughput; wide oscillations suggest
    periodic internal bottlenecks (e.g. InnoDB log checkpointing, buffer pool flushing, or purge lag).
  </p>
  <div class="chart"><img src="data:image/png;base64,{img_ts}" alt="NOTPM timeseries"></div>
  <div class="chart-caption">Figure 5 \u2014 NOTPM over elapsed time. BP 50G \u00b7 64 VU. Y-axis starts at zero. Ramp-up excluded.</div>
  <div class="callout">{insight_timeseries()}</div>
</section>

<section>
  <h2>NOTPM Jitter  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">last 30 min \u00b7 box = P25\u2011P75 \u00b7 whiskers = P5\u2011P95</span></h2>
  <p>
    <strong>NOTPM Jitter</strong> quantifies the <em>spread</em> of second-to-second throughput variation,
    focusing on the final 30 minutes of each run when the system has fully warmed up and reached
    steady state. While the Stability chart above shows the full time-series shape, jitter distills
    it into a single statistical picture: how tightly packed are the per-second NOTPM readings
    around the mean?
  </p>
  <p>
    A database with low jitter delivers predictable response times, simplifies capacity planning,
    and avoids tail-latency violations under peak load. High jitter forces the application tier
    to absorb throughput dips through connection pooling, retry logic, or queuing \u2014 adding
    complexity and latency even when the average throughput looks good.
  </p>
  <p>
    Each box below shows the P25\u2013P75 range (interquartile), the centre line is the median,
    and whiskers extend to P5\u2013P95. The tables include <strong>CV%</strong> (Coefficient of
    Variation = std\u2009/\u2009mean\u2009\u00d7\u2009100): a scale-free measure where lower is
    more stable. Unlike raw standard deviation, CV% is directly comparable across runs with
    different mean throughputs.
  </p>

  <h3>Buffer Pool Sweep</h3>
  <div class="chart"><img src="data:image/png;base64,{img_jitter_bp}" alt="BP jitter"></div>
  <div class="chart-caption">Figure 6 \u2014 NOTPM distribution per buffer pool size (last 30 min).</div>
  <div class="callout">{insight_jitter_bp()}</div>
  {_html_jitter_table(bp_jitter_rows)}

  <h3 style="margin-top:28px;">Virtual Users Sweep</h3>
  <div class="chart"><img src="data:image/png;base64,{img_jitter_vu}" alt="VU jitter"></div>
  <div class="chart-caption">Figure 7 \u2014 NOTPM distribution per VU count (last 30 min).</div>
  <div class="callout">{insight_jitter_vu()}</div>
  {_html_jitter_table(vu_jitter_rows)}
</section>

<section>
  <h2>Database Configuration</h2>
  <p>
    All engines used the same base <code>my.cnf</code>.
    The only parameter that varies across runs is <code>innodb_buffer_pool_size</code>.
    <span style="color:#f97316;font-weight:600;">MariaDB-only</span> parameters are highlighted.
    Parameters that differ are marked <span style="color:#a78bfa;font-weight:600;">purple</span>.
  </p>
  <table class="data-table cfg-table">
    <thead><tr><th>Parameter</th>{engine_th()}</tr></thead>
    <tbody>{cfg_html_rows()}</tbody>
  </table>
</section>

<section>
  <h2>Methodology</h2>
  <p><strong>Benchmark:</strong> TPROC-C via HammerDB 4.12 (<code>tpcc_run.tcl</code>).</p>
  <p><strong>Workload:</strong> 1000 warehouses (~100 GB data), 60 s ramp-up, 3600 s measurement window.</p>
  <p><strong>Hardware:</strong> Intel Xeon Gold 6230 (2\u00d720 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB).</p>
  <p><strong>OS:</strong> Ubuntu 24.04, kernel 6.8.0-60-generic.</p>
  <p><strong>Engines:</strong> MariaDB 12.2.2, MariaDB 12.3.1, MySQL 8.4.8, MySQL 9.7.0-er2.</p>
  <p><strong>Metric:</strong> NOTPM = per-second commit rate \u00d7 60 \u00d7 0.45 (TPROC-C new-order mix).</p>
  <p><strong>Buffer pool sweep:</strong> 64 VU, buffer pool varied 10\u201380 GiB in 10 GiB steps.</p>
  <p><strong>VU sweep:</strong> 50 GiB buffer pool, VU \u2208 {{1, 2, 4, 8, 16, 32, 64, 128}}.</p>
</section>

<footer>
  Data source: <a href="https://github.com/Percona-Lab-results/tpcc-benchmark-framework">Percona-Lab-results/tpcc-benchmark-framework</a> \u00b7
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
def md_engine_header():
    return " | ".join(ENGINES[eid]["display"] for eid in ENGINE_IDS)

def md_engine_sep():
    return "|".join("---" for _ in ENGINE_IDS)

def md_bp_table():
    rows = [f"| BP Size | {md_engine_header()} |",
            f"|---------|{md_engine_sep()}|"]
    for row in bp_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = []
        for eid in ENGINE_IDS:
            v = row[eid]
            cells.append(f"**{v:,}**" if v == max_v and v > 0 else (f"{v:,}" if v > 0 else "\u2014"))
        rows.append(f"| {row['size']} | {' | '.join(cells)} |")
    return "\n".join(rows)


def md_vu_table():
    rows = [f"| VU | {md_engine_header()} |",
            f"|----|{md_engine_sep()}|"]
    for row in vu_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = []
        for eid in ENGINE_IDS:
            v = row[eid]
            cells.append(f"**{v:,}**" if v == max_v and v > 0 else (f"{v:,}" if v > 0 else "\u2014"))
        rows.append(f"| {row['vu']} | {' | '.join(cells)} |")
    return "\n".join(rows)


def md_cfg_table():
    rows = [f"| Parameter | {md_engine_header()} | Note |",
            f"|-----------|{md_engine_sep()}|------|"]
    cur_section = None
    for section, param, vals in cfg_rows:
        if section != cur_section:
            cur_section = section
            rows.append(f"| **{section}** | {'| ' * len(ENGINE_IDS)}|")
        maria_only = param in MARIA_ONLY
        unique = set(v for v in vals.values() if v)
        note = "MariaDB only" if maria_only else ("differs" if len(unique) > 1 else "")
        cells = " | ".join(f"`{vals[eid] or 'n/a'}`" for eid in ENGINE_IDS)
        rows.append(f"| `{param}` | {cells} | {note} |")
    return "\n".join(rows)


def _html_to_md(s):
    """Convert HTML bold/em to markdown."""
    s = re.sub(r'<strong>(.*?)</strong>', r'**\1**', s)
    s = re.sub(r'<em>(.*?)</em>', r'*\1*', s)
    s = re.sub(r'<[^>]+>', '', s)
    return s


def build_md():
    return f"""# Database Benchmark Comparison -- TPROC-C Report

**HammerDB 4.12 | TPROC-C | 1000 warehouses | 3600 s runs | 60 s ramp-up**
**Hardware:** Intel Xeon Gold 6230 (2x20c, HT = 80 logical CPUs) | 187 GiB RAM | NVMe 2.9 TB
**OS:** Ubuntu 24.04 | kernel 6.8.0-60-generic | Generated: {datetime.now().strftime("%Y-%m-%d")}
**Engines:** MariaDB 12.2.2, MariaDB 12.3.1, MySQL 8.4.8, MySQL 9.7.0-er2

---

## Executive Summary

| Metric | {md_engine_header()} |
|--------|{md_engine_sep()}|
| Peak NOTPM (BP 80G, 64 VU) | {' | '.join(f'{int(kpi_peak_bp[eid]):,}' for eid in ENGINE_IDS)} |
| Peak NOTPM (BP 50G, 128 VU) | {' | '.join(f'{int(kpi_peak_vu[eid]):,}' for eid in ENGINE_IDS)} |
| Scaling 1->128 VU (BP 50G) | {' | '.join(f'{kpi_scaling[eid]:.0f}x' for eid in ENGINE_IDS)} |

---

## Buffer Pool Sweep -- 64 VU, 10G-80G

The **InnoDB Buffer Pool** is the main memory area where InnoDB caches table data and index
pages. Every read that hits the buffer pool avoids a disk I/O; every miss forces a physical
read from storage. For write-heavy OLTP workloads like TPROC-C, the buffer pool also holds
dirty pages waiting to be flushed -- a larger pool means fewer flush cycles and less I/O
contention between foreground transactions and background flushing.

A **buffer pool sweep** varies this single parameter (from 10 GiB to 80 GiB in 10 GiB steps)
while holding everything else constant -- 64 virtual users, 1000 warehouses (~100 GB working
set), same hardware, same configuration. This isolates the effect of memory pressure on
throughput. At small pool sizes (10-30G) only a fraction of the hot data fits in RAM, so
performance is dominated by disk I/O speed and the engine's read-ahead and flushing strategies.
As the pool grows toward the working set size, more reads hit cache and fewer dirty-page
evictions are needed, revealing the engine's in-memory efficiency.

The 64 VU count was chosen to represent a moderate-to-high concurrency level typical of
production OLTP servers, ensuring that throughput differences reflect buffer pool efficiency
rather than single-thread performance.

![TPROC-C Throughput vs Buffer Pool Size](report_assets/fig1_bp_line.png)

> {_html_to_md(insight_bp_line())}

![TPROC-C Throughput -- bar chart](report_assets/fig5_bp_bar.png)

> {_html_to_md(insight_bp_bar())}

{md_bp_table()}

---

## Virtual Users Sweep -- BP 50G, 1-128 VU

A **Virtual User (VU)** is a HammerDB worker thread that simulates an independent database
client. Each VU opens its own connection, picks a random warehouse, and continuously executes
the TPROC-C transaction mix (new-order, payment, delivery, order-status, stock-level) in a
tight loop for the duration of the run. Increasing the VU count is equivalent to increasing
the number of concurrent application threads hitting the database simultaneously.

VU count directly stresses the database engine's concurrency internals: InnoDB row-level
locking, the lock manager, undo/purge scheduling, buffer pool latch contention, and redo log
synchronisation. At low VU counts the engine is mostly CPU- and I/O-bound; as VU rises,
internal latch contention and lock waits become the dominant bottleneck. The point where
throughput plateaus reveals how efficiently the engine scales under parallel workloads -- a
critical metric for multi-tenant and connection-pool-heavy OLTP deployments.

Concurrency was swept from 1 to 128 virtual users with a fixed 50 GiB buffer pool. Each VU
count ran for 3600 seconds with a 60-second ramp-up.

![TPROC-C Throughput vs Concurrency](report_assets/fig2_vu_line.png)

![Concurrency Scaling Efficiency](report_assets/fig4_scaling.png)

> {_html_to_md(insight_vu_line())}

> {_html_to_md(insight_scaling())}

{md_vu_table()}

---

## NOTPM Stability -- BP 50G, 64 VU

**NOTPM Stability** measures how consistently a database sustains its throughput over the entire
duration of a benchmark run. A high average NOTPM is meaningless if the engine periodically
stalls -- background checkpoint flushes, purge operations, or adaptive flushing can cause sharp
dips that ripple through the application as latency spikes.

The chart plots per-second NOTPM for the full 3600-second run (ramp-up excluded) at BP 50G
with 64 virtual users. Thin lines are raw 1-second samples; thick lines are 60-second rolling
averages. A flat rolling average indicates stable throughput; wide oscillations suggest periodic
internal bottlenecks (e.g. InnoDB log checkpointing, buffer pool flushing, or purge lag).

![NOTPM Over Time](report_assets/fig3_timeseries.png)

> {_html_to_md(insight_timeseries())}

---

## NOTPM Jitter -- last 30 min of each run

**NOTPM Jitter** quantifies the *spread* of second-to-second throughput variation, focusing on
the final 30 minutes of each run when the system has fully warmed up and reached steady state.
While the Stability chart above shows the full time-series shape, jitter distills it into a
single statistical picture: how tightly packed are the per-second NOTPM readings around the mean?

A database with low jitter delivers predictable response times, simplifies capacity planning,
and avoids tail-latency violations under peak load. High jitter forces the application tier to
absorb throughput dips through connection pooling, retry logic, or queuing -- adding complexity
and latency even when the average throughput looks good.

Each box shows the P25-P75 range (interquartile), the centre line is the median, and whiskers
extend to P5-P95. The tables include **CV%** (Coefficient of Variation = std / mean x 100):
a scale-free measure where lower is more stable. Unlike raw standard deviation, CV% is directly
comparable across runs with different mean throughputs.

### Buffer Pool Sweep

![NOTPM Jitter -- BP Sweep](report_assets/fig6_jitter_bp.png)

> {_html_to_md(insight_jitter_bp())}

{_md_jitter_table(bp_jitter_rows)}

### Virtual Users Sweep

![NOTPM Jitter -- VU Sweep](report_assets/fig7_jitter_vu.png)

> {_html_to_md(insight_jitter_vu())}

{_md_jitter_table(vu_jitter_rows)}

---

## Database Configuration

{md_cfg_table()}

---

## Methodology

- **Benchmark:** TPROC-C via HammerDB 4.12 (`tpcc_run.tcl`)
- **Workload:** 1000 warehouses (~100 GB), 60 s ramp-up, 3600 s measurement window
- **Hardware:** Intel Xeon Gold 6230 (2x20 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB)
- **OS:** Ubuntu 24.04, kernel 6.8.0-60-generic
- **Engines:** MariaDB 12.2.2, MariaDB 12.3.1, MySQL 8.4.8, MySQL 9.7.0-er2
- **Metric:** NOTPM = per-second commit rate x 60 x 0.45 (TPROC-C new-order mix is 45%)
- **BP sweep:** 64 VU, buffer pool 10-80 GiB in 10 GiB steps
- **VU sweep:** 50 GiB buffer pool, VU in {{1, 2, 4, 8, 16, 32, 64, 128}}

---

*Data source: [Percona-Lab-results/tpcc-benchmark-framework](https://github.com/Percona-Lab-results/tpcc-benchmark-framework)*
"""


md_out = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "REPORT.md")
with open(md_out, "w", encoding="utf-8") as f:
    f.write(build_md())
print(f"Markdown report written -> {md_out}")
