"""
Build static HTML report comparing MariaDB 12.2, MariaDB 12.3, MySQL 8.4, MySQL 9.7
from BP iterations and VU iterations benchmark runs.
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
        "color":    "#d95f02",    # dark orange
        "marker":   "o",
        "css":      "maria122",
        "pill_cls": "pill-maria122",
    }),
    ("maria123", {
        "display":  "MariaDB 12.3.1",
        "short":    "MDB 12.3",
        "color":    "#e6ab02",    # dark gold
        "marker":   "D",
        "css":      "maria123",
        "pill_cls": "pill-maria123",
    }),
    ("mysql84", {
        "display":  "MySQL 8.4.8",
        "short":    "MySQL 8.4",
        "color":    "#1b9e77",    # teal
        "marker":   "s",
        "css":      "mysql84",
        "pill_cls": "pill-mysql84",
    }),
    ("mysql97", {
        "display":  "MySQL 9.7.0",
        "short":    "MySQL 9.7",
        "color":    "#7570b3",    # muted indigo
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

# ── theme (light / Google Docs) ───────────────────────────────────────────────
C_BG     = "#ffffff"
C_CARD   = "#ffffff"
C_GRID   = "#e0e0e0"
C_FG     = "#1a1a1a"
C_DIM    = "#555555"
C_AXIS   = "#cccccc"

plt.rcParams.update({
    "figure.facecolor":  C_BG,
    "axes.facecolor":    C_CARD,
    "axes.edgecolor":    C_AXIS,
    "axes.labelcolor":   C_DIM,
    "text.color":        C_FG,
    "xtick.color":       C_DIM,
    "ytick.color":       C_DIM,
    "xtick.major.size":  4,
    "ytick.major.size":  4,
    "grid.color":        C_GRID,
    "grid.linewidth":    0.7,
    "legend.facecolor":  C_CARD,
    "legend.edgecolor":  C_AXIS,
    "legend.framealpha": 1.0,
    "legend.fontsize":   10,
    "font.family":       "DejaVu Sans",
    "font.size":         9,
    "axes.titlesize":    11,
    "axes.titlepad":     12,
    "axes.labelsize":    9,
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
    fig.savefig(buf, format="png", dpi=144, bbox_inches="tight", pad_inches=0.15, facecolor="white")
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
    ax.yaxis.grid(True, color=C_GRID, lw=0.6, ls="-", alpha=0.7)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.tick_params(axis="both", length=4, pad=6, colors=C_DIM)


def pct_diff(a, b):
    if b == 0:
        return "\u2014"
    d = (a - b) / b * 100
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.1f}%"


# ══════════════════════════════════════════════════════════════════════════════
#  DATASET 1 — BP iterations  (64 VU, BP 10–80 G)
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
#  DATASET 2 — VU iterations  (BP 50G, VU 1–128)
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
#  FIGURE 1 — BP iterations line chart
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_chart():
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys = bp_series[eid]
        if not xs:
            continue
        ys_k = [y / 1000 for y in ys]
        ax.plot(xs, ys_k, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor="white",
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)

    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Buffer Pool Size  [64 VU \u00b7 3600 s]")
    ax.set_xticks(all_bp_sizes)
    ax.set_xticklabels([f"{x}G" for x in all_bp_sizes])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left", fontsize=8)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig1_bp_line.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 2 — VU iterations line chart
# ══════════════════════════════════════════════════════════════════════════════
def make_vu_chart():
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys = vu_data[eid]
        if not xs:
            continue
        ys_k = [y / 1000 for y in ys]
        ax.plot(xs, ys_k, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor="white",
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)

    ax.set_xlabel("Virtual Users (log scale)")
    ax.set_ylabel("Average NOTPM (thousands)")
    ax.set_title("TPROC-C Throughput vs Concurrency  [BP 50G \u00b7 3600 s]")
    ax.set_xscale("log", base=2)
    ax.set_xticks(all_vus)
    ax.set_xticklabels([str(x) for x in all_vus])
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0f}k"))
    ax.legend(loc="upper left", fontsize=8)
    ax.set_ylim(bottom=0)
    _clean_axes(ax)
    fig.tight_layout(pad=1.5)
    return fig_to_b64(fig, "fig2_vu_line.png")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 3 — TPS time-series (BP 50G, 64 VU)
# ══════════════════════════════════════════════════════════════════════════════
def make_timeseries_chart():
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        run = ts_runs[eid]
        if run is None:
            continue
        et, tps = qps_timeseries(run)
        if not et:
            continue
        smooth = rolling_avg(tps, window=60)
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
    fig, ax = plt.subplots(figsize=(11.5, 6.5))

    ax.plot([1, 128], [1, 128], color=C_AXIS, lw=1.5, ls="--",
            label="Linear (ideal)", alpha=0.5, zorder=1)
    for eid in ENGINE_IDS:
        e = ENGINES[eid]
        xs, ys, _ = eff_data[eid]
        if not xs:
            continue
        ax.plot(xs, ys, color=e["color"], lw=2.5,
                marker=e["marker"], ms=7, markerfacecolor="white",
                markeredgecolor=e["color"], markeredgewidth=2,
                label=e["display"], zorder=5)

    # CPU topology reference lines
    ax.axvline(x=40, color="#d32f2f", lw=1.3, ls="-.", alpha=0.6, zorder=2)
    ax.text(40, ax.get_ylim()[0] if ax.get_ylim()[0] > 0 else 1, "  40 physical cores",
            color="#d32f2f", fontsize=7.5, va="bottom", ha="left", alpha=0.8)
    ax.axvline(x=80, color="#1565c0", lw=1.3, ls="-.", alpha=0.6, zorder=2)
    ax.text(80, ax.get_ylim()[0] if ax.get_ylim()[0] > 0 else 1, "  80 HT threads",
            color="#1565c0", fontsize=7.5, va="bottom", ha="left", alpha=0.8)

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
#  FIGURE 5 — BP iterations: grouped bar
# ══════════════════════════════════════════════════════════════════════════════
def make_bp_bar_chart():
    n_engines = len(ENGINE_IDS)
    x = np.arange(len(all_bp_sizes))
    w = 0.8 / n_engines

    fig, ax = plt.subplots(figsize=(11.5, 6.5))
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
    ax.set_title("TPROC-C Throughput \u2014 Buffer Pool Iterations  [64 VU \u00b7 3600 s]")
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
    """Pick representative run: best NOTPM run from BP 80G iteration for the engine."""
    cands = [
        r for r in runs
        if r["_eid"] == eid and "80G" in r["label"] and "sweep" in r["label"].lower()
    ]
    if not cands:
        # fallback: any iteration run
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
                medianprops=dict(color="#333333", lw=2),
                boxprops=dict(facecolor=col + "30", alpha=1, linewidth=1.3, edgecolor=col),
                whiskerprops=dict(color=col, lw=1.2, alpha=0.8, linestyle=(0, (4, 3))),
                capprops=dict(color=col, lw=1.6),
                manage_ticks=False,
            )


def make_jitter_bp_chart():
    sizes = all_bp_sizes
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    _boxplot_group(ax, sizes, bp_jitter)
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f"{s}G" for s in sizes])
    ax.set_xlabel("InnoDB Buffer Pool Size (GiB)")
    ax.set_ylabel("NOTPM (thousands) \u2014 last 30 min")
    ax.set_title("NOTPM Jitter \u2014 Buffer Pool Iterations  [64 VU \u00b7 last 30 min of each run]")
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
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    _boxplot_group(ax, vus, vu_jitter)
    ax.set_xticks(range(len(vus)))
    ax.set_xticklabels([str(v) for v in vus])
    ax.set_xlabel("Virtual Users")
    ax.set_ylabel("NOTPM (thousands) \u2014 last 30 min")
    ax.set_title("NOTPM Jitter \u2014 VU Iterations  [BP 50G \u00b7 last 30 min of each run]")
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


HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Database Benchmark Comparison \u2014 TPROC-C Report</title>
<style>
  body {{
    font-family: Arial, Helvetica, sans-serif;
    background: #ffffff;
    color: #222222;
    line-height: 1.6;
    max-width: 900px;
    margin: 0 auto;
    padding: 20px;
  }}
  a {{ color: #1a73e8; }}
  h1 {{ font-size: 22pt; color: #1a1a1a; margin-bottom: 8px; }}
  h2 {{ font-size: 14pt; color: #333; margin-top: 32px; border-bottom: 2px solid #ddd; padding-bottom: 6px; }}
  h3 {{ font-size: 12pt; color: #555; margin-top: 24px; }}
  p {{ font-size: 11pt; color: #333; margin-bottom: 10px; line-height: 1.7; }}
  .subtitle {{ color: #666; font-size: 10pt; margin-bottom: 16px; }}
  .pills {{ margin: 12px 0 24px; }}
  .pill {{
    display: inline-block;
    font-size: 9pt;
    padding: 2px 10px;
    border-radius: 12px;
    border: 1px solid #ccc;
    color: #555;
    margin: 2px 4px 2px 0;
  }}
  img {{ max-width: 100%; height: auto; margin: 16px 0; }}
  .chart-caption {{ font-size: 9pt; color: #888; font-style: italic; margin-bottom: 20px; }}

  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 10pt;
    margin: 16px 0;
  }}
  th {{
    text-align: left;
    background: #f5f5f5;
    padding: 8px 10px;
    border: 1px solid #ddd;
    font-weight: 700;
    font-size: 9pt;
    color: #444;
  }}
  td {{
    padding: 6px 10px;
    border: 1px solid #ddd;
  }}
  td.win {{ font-weight: 700; }}
  tr:nth-child(even) td {{ background: #fafafa; }}

  .callout {{
    background: #f0f4f8;
    border-left: 4px solid #4a90d9;
    padding: 12px 16px;
    font-size: 10.5pt;
    color: #333;
    margin: 16px 0;
  }}
  .kpi-grid {{ margin: 16px 0; }}
  .kpi {{
    display: inline-block;
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 4px 8px 4px 0;
    min-width: 180px;
    vertical-align: top;
  }}
  .kpi .kpi-label {{ font-size: 8pt; color: #777; text-transform: uppercase; }}
  .kpi .kpi-val {{ font-size: 16pt; font-weight: 700; margin: 4px 0; }}
  .kpi .kpi-sub {{ font-size: 8pt; color: #999; }}

  .cfg-section td {{ background: #f0f0f0; font-weight: 700; font-size: 9pt; color: #555; }}
  .cfg-param {{ font-family: 'Consolas','Courier New',monospace; }}
  .cfg-maria td {{ background: #fff8f0; }}
  .cfg-maria .cfg-param {{ color: #c45000; }}
  .cfg-diff td {{ background: #f5f0ff; }}
  .cfg-diff .cfg-param {{ color: #6b21a8; }}
  .cfg-na {{ color: #bbb; font-style: italic; }}
  .badge-maria {{
    font-size: 7pt;
    background: #fff3e0;
    border: 1px solid #e6a04080;
    color: #c45000;
    padding: 1px 4px;
    border-radius: 3px;
    margin-left: 4px;
  }}
  footer {{ border-top: 1px solid #ddd; padding-top: 16px; font-size: 9pt; color: #999; text-align: center; margin-top: 40px; }}
</style>
</head>
<body>

<h1>Database Benchmark Comparison \u2014 TPROC-C Report</h1>
<div class="subtitle">HammerDB 4.12 \u00b7 TPROC-C \u00b7 1000 warehouses \u00b7 Intel Xeon Gold 6230 (2\u00d720c) \u00b7 187 GiB RAM \u00b7 NVMe 2.9 TB</div>
<div class="pills">
  {pills_html()}
</div>

<h2>About HammerDB</h2>
<p>
  <strong>HammerDB</strong> is an open-source database benchmarking tool that simulates real-world
  transactional workloads against relational databases. It implements industry-standard benchmarks
  including <strong>TPROC-C</strong> (derived from TPC-C), which models an order-processing warehouse
  system \u2014 one of the most widely used OLTP benchmarks for evaluating database throughput,
  concurrency scaling, and latency under load.
</p>
<p>
  In a TPROC-C run, HammerDB spawns multiple <strong>virtual users (VUs)</strong>, each acting as an
  independent client that continuously executes a mix of five transaction types: new-order (45%),
  payment (43%), order-status (4%), delivery (4%), and stock-level (4%). The primary metric is
  <strong>NOTPM</strong> (New Orders Per Minute), derived from the per-second transaction commit rate.
  This report uses NOTPM = commits/s \u00d7 60 \u00d7 0.45 to reflect the new-order transaction mix.
</p>


  <h2>Buffer Pool Iterations  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">64 VU \u00b7 10G \u2013 80G</span></h2>
  <p>
    The <strong>InnoDB Buffer Pool</strong> is the main memory area where InnoDB caches table data
    and index pages. Every read that hits the buffer pool avoids a disk I/O; every miss forces a
    physical read from storage. For write-heavy OLTP workloads like TPROC-C, the buffer pool also
    holds dirty pages waiting to be flushed \u2014 a larger pool means fewer flush cycles and less
    I/O contention between foreground transactions and background flushing.
  </p>
  <p>
    A <strong>buffer pool iteration</strong> varies this single parameter (from 10 GiB to 80 GiB in
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
  <img width="899" src="data:image/png;base64,{img_bp_line}" alt="BP iterations line chart">
  <div class="chart-caption">Figure 1 \u2014 Average NOTPM vs buffer pool size. Each point is the steady-state average (post-ramp-up).</div>


  <h2>Virtual Users Iterations  <span style="font-weight:400;color:#3d5070;font-size:0.8rem">BP 50G \u00b7 1 \u2013 128 VU</span></h2>
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
    Concurrency was iterated from 1 to 128 virtual users with a fixed 50 GiB buffer pool.
    Each VU count ran for 3600 seconds with a 60-second ramp-up.
  </p>
  <img width="899" src="data:image/png;base64,{img_vu_line}" alt="VU iterations line chart">
  <div class="chart-caption">Figure 3 \u2014 NOTPM vs virtual users (log\u2082 X-axis).</div>
  <img width="899" src="data:image/png;base64,{img_scaling}" alt="Scaling efficiency">
  <div class="chart-caption">Figure 4 \u2014 Speedup vs 1 VU on log/log axes. Dashed = ideal linear scaling. Vertical lines mark physical core count (40) and HT thread count (80).</div>
  <p>
    <strong>Note on scaling vs peak throughput:</strong> MySQL 9.7 delivers the highest absolute
    NOTPM at every concurrency level, yet its relative scaling factor (1\u2192128 VU) is lower
    than MySQL 8.4. This is expected: MySQL 9.7 starts from a significantly higher single-thread
    baseline, so it saturates the available CPU resources sooner in relative terms. Beyond the
    physical core count (40 cores / 80 HT threads on this system), even a perfectly scalable
    engine cannot maintain linear speedup \u2014 threads begin competing for the same execution
    units, and InnoDB internal serialisation points (lock manager, redo log, buffer pool latches)
    become the bottleneck. A higher baseline simply means the engine hits that ceiling at a lower
    multiplier, not that it scales worse in absolute terms.
  </p>

  <h3>Virtual Users Iterations \u2014 Data Table</h3>
  <table class="data-table">
    <thead><tr><th>VU</th>{engine_th()}</tr></thead>
    <tbody>{vu_html_rows()}</tbody>
  </table>


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
  <img width="899" src="data:image/png;base64,{img_ts}" alt="NOTPM timeseries">
  <div class="chart-caption">Figure 5 \u2014 NOTPM over elapsed time. BP 50G \u00b7 64 VU. Y-axis starts at zero. Ramp-up excluded.</div>


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

  <h3>Buffer Pool Iterations</h3>
  <img width="899" src="data:image/png;base64,{img_jitter_bp}" alt="BP jitter">
  <div class="chart-caption">Figure 6 \u2014 NOTPM distribution per buffer pool size (last 30 min).</div>
  <p>
    At small buffer pool sizes (10\u201350G), both MariaDB versions exhibit noticeably wider
    NOTPM spread (CV 12\u201326%) compared to MySQL 8.4 and 9.7 (CV 7\u201310%). This suggests
    more aggressive checkpoint flushing and dirty-page eviction under memory pressure in
    MariaDB, which creates periodic throughput dips. As the buffer pool approaches the working
    set size (70\u201380G), all four engines converge to similar jitter levels (CV 4\u20137%),
    confirming that the instability is I/O-driven rather than an inherent engine limitation.
    MySQL 8.4 stands out as the most consistently stable across all buffer pool sizes.
  </p>

  <h3 style="margin-top:28px;">Virtual Users Iterations</h3>
  <img width="899" src="data:image/png;base64,{img_jitter_vu}" alt="VU jitter">
  <div class="chart-caption">Figure 7 \u2014 NOTPM distribution per VU count (last 30 min).</div>
  <p>
    Jitter increases with concurrency for all engines, but the divergence is striking: at
    64\u2013128 VU, both MariaDB versions reach CV 24\u201326%, while MySQL 8.4 stays at
    8\u201310% and MySQL 9.7 at 10\u201311%. At low concurrency (1\u20138 VU), all engines
    are tightly clustered below CV 7%, indicating the gap is driven by internal contention
    under heavy parallelism \u2014 likely lock manager scheduling, purge thread interference,
    or adaptive flushing behaviour. For latency-sensitive applications, MySQL\u2019s lower
    jitter at high concurrency translates directly to more predictable response times and
    fewer tail-latency violations.
  </p>


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


  <h2>Methodology</h2>
  <p><strong>Benchmark:</strong> TPROC-C via HammerDB 4.12 (<code>tpcc_run.tcl</code>).</p>
  <p><strong>Workload:</strong> 1000 warehouses (~100 GB data), 60 s ramp-up, 3600 s measurement window.</p>
  <p><strong>Hardware:</strong> Intel Xeon Gold 6230 (2\u00d720 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB).</p>
  <p><strong>OS:</strong> Ubuntu 24.04, kernel 6.8.0-60-generic.</p>
  <p><strong>Engines:</strong> MariaDB 12.2.2, MariaDB 12.3.1, MySQL 8.4.8, MySQL 9.7.0-er2.</p>
  <p><strong>Metric:</strong> NOTPM = per-second commit rate \u00d7 60 \u00d7 0.45 (TPROC-C new-order mix).</p>
  <p><strong>Buffer pool iterations:</strong> 64 VU, buffer pool varied 10\u201380 GiB in 10 GiB steps.</p>
  <p><strong>VU iterations:</strong> 50 GiB buffer pool, VU \u2208 {{1, 2, 4, 8, 16, 32, 64, 128}}.</p>

<footer>
  Data source: <a href="https://github.com/Percona-Lab-results/tpcc-benchmark-framework">Percona-Lab-results/tpcc-benchmark-framework</a> \u00b7
  Report generated {datetime.now().strftime("%Y-%m-%d %H:%M")}
</footer>

</body>
</html>
"""

out = "report_gdoc_v2.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"Google Docs report written -> {out}")

# ── Markdown report (REPORT.md) ──────────────────────────────────────────────
def _md_bp_table():
    lines = [
        f"| BP Size | {' | '.join(ENGINES[e]['display'] for e in ENGINE_IDS)} |",
        f"|---------|{'|'.join('---|' for _ in ENGINE_IDS)}",
    ]
    for row in bp_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = [f"**{v:,}**" if v == max_v and v > 0 else (f"{v:,}" if v > 0 else "\u2014") for v in vals]
        lines.append(f"| {row['size']} | {' | '.join(cells)} |")
    return "\n".join(lines)


def _md_vu_table():
    lines = [
        f"| VU | {' | '.join(ENGINES[e]['display'] for e in ENGINE_IDS)} |",
        f"|----|{'|'.join('---|' for _ in ENGINE_IDS)}",
    ]
    for row in vu_table_rows:
        vals = [row[eid] for eid in ENGINE_IDS]
        max_v = max(vals) if any(v > 0 for v in vals) else 0
        cells = [f"**{v:,}**" if v == max_v and v > 0 else (f"{v:,}" if v > 0 else "\u2014") for v in vals]
        lines.append(f"| {row['vu']} | {' | '.join(cells)} |")
    return "\n".join(lines)


def _md_exec_summary():
    lines = [
        f"| Metric | {' | '.join(ENGINES[e]['display'] for e in ENGINE_IDS)} |",
        f"|--------|{'|'.join('---|' for _ in ENGINE_IDS)}",
        f"| Peak NOTPM (BP 80G, 64 VU) | {' | '.join(f'{int(kpi_peak_bp[e]):,}' for e in ENGINE_IDS)} |",
        f"| Peak NOTPM (BP 50G, 128 VU) | {' | '.join(f'{int(kpi_peak_vu[e]):,}' for e in ENGINE_IDS)} |",
        f"| Scaling 1->128 VU (BP 50G) | {' | '.join(f'{kpi_scaling[e]:.0f}x' for e in ENGINE_IDS)} |",
    ]
    return "\n".join(lines)


# Build config table for markdown
def _md_cfg_table():
    lines = [
        f"| Parameter | {' | '.join(ENGINES[e]['display'] for e in ENGINE_IDS)} | Note |",
        f"|-----------|{'|'.join('---|' for _ in ENGINE_IDS)}------|",
    ]
    cur_section = None
    for section, param, vals in cfg_rows:
        if section != cur_section:
            cur_section = section
            lines.append(f"| **{section}** | {'| '.join('' for _ in ENGINE_IDS)}| |")
        maria_only = param in MARIA_ONLY
        note = "MariaDB only" if maria_only else ""
        cells = [vals[eid] if vals[eid] else "" for eid in ENGINE_IDS]
        lines.append(f"| `{param}` | {' | '.join(f'`{c}`' if c else '' for c in cells)} | {note} |")
    return "\n".join(lines)


REPORT_MD = f"""# Database Benchmark Comparison -- TPROC-C Report

**HammerDB 4.12 | TPROC-C | 1000 warehouses | 3600 s runs | 60 s ramp-up**
**Hardware:** Intel Xeon Gold 6230 (2x20c, HT = 80 logical CPUs) | 187 GiB RAM | NVMe 2.9 TB
**OS:** Ubuntu 24.04 | kernel 6.8.0-60-generic | Generated: {datetime.now().strftime("%Y-%m-%d")}
**Engines:** {", ".join(ENGINES[e]["display"] for e in ENGINE_IDS)}

---

## Executive Summary

{_md_exec_summary()}

---

## Buffer Pool Iterations -- 64 VU, 10G-80G

The **InnoDB Buffer Pool** is the main memory area where InnoDB caches table data and index
pages. Every read that hits the buffer pool avoids a disk I/O; every miss forces a physical
read from storage. For write-heavy OLTP workloads like TPROC-C, the buffer pool also holds
dirty pages waiting to be flushed -- a larger pool means fewer flush cycles and less I/O
contention between foreground transactions and background flushing.

A **buffer pool iteration** varies this single parameter (from 10 GiB to 80 GiB in 10 GiB steps)
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

{_md_bp_table()}

---

## Virtual Users Iterations -- BP 50G, 1-128 VU

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

Concurrency was iterated from 1 to 128 virtual users with a fixed 50 GiB buffer pool. Each VU
count ran for 3600 seconds with a 60-second ramp-up.

![TPROC-C Throughput vs Concurrency](report_assets/fig2_vu_line.png)

![Concurrency Scaling Efficiency](report_assets/fig4_scaling.png)

**Note on scaling vs peak throughput:** MySQL 9.7 delivers the highest absolute NOTPM at every
concurrency level, yet its relative scaling factor (1->128 VU) is lower than MySQL 8.4. This is
expected: MySQL 9.7 starts from a significantly higher single-thread baseline, so it saturates
the available CPU resources sooner in relative terms. Beyond the physical core count (40 cores /
80 HT threads on this system), even a perfectly scalable engine cannot maintain linear speedup --
threads begin competing for the same execution units, and InnoDB internal serialisation points
(lock manager, redo log, buffer pool latches) become the bottleneck. A higher baseline simply
means the engine hits that ceiling at a lower multiplier, not that it scales worse in absolute
terms.

{_md_vu_table()}

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

### Buffer Pool Iterations

![NOTPM Jitter -- BP Iterations](report_assets/fig6_jitter_bp.png)

At small buffer pool sizes (10-50G), both MariaDB versions exhibit noticeably wider NOTPM spread
(CV 12-26%) compared to MySQL 8.4 and 9.7 (CV 7-10%). This suggests more aggressive checkpoint
flushing and dirty-page eviction under memory pressure in MariaDB, which creates periodic
throughput dips. As the buffer pool approaches the working set size (70-80G), all four engines
converge to similar jitter levels (CV 4-7%), confirming that the instability is I/O-driven rather
than an inherent engine limitation. MySQL 8.4 stands out as the most consistently stable across
all buffer pool sizes.

{_md_jitter_table(bp_jitter_rows)}

### Virtual Users Iterations

![NOTPM Jitter -- VU Iterations](report_assets/fig7_jitter_vu.png)

Jitter increases with concurrency for all engines, but the divergence is striking: at 64-128 VU,
both MariaDB versions reach CV 24-26%, while MySQL 8.4 stays at 8-10% and MySQL 9.7 at 10-11%.
At low concurrency (1-8 VU), all engines are tightly clustered below CV 7%, indicating the gap
is driven by internal contention under heavy parallelism -- likely lock manager scheduling, purge
thread interference, or adaptive flushing behaviour. For latency-sensitive applications, MySQL's
lower jitter at high concurrency translates directly to more predictable response times and fewer
tail-latency violations.

{_md_jitter_table(vu_jitter_rows)}

---

## Database Configuration

{_md_cfg_table()}

---

## Methodology

- **Benchmark:** TPROC-C via HammerDB 4.12 (`tpcc_run.tcl`)
- **Workload:** 1000 warehouses (~100 GB), 60 s ramp-up, 3600 s measurement window
- **Hardware:** Intel Xeon Gold 6230 (2x20 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB)
- **OS:** Ubuntu 24.04, kernel 6.8.0-60-generic
- **Engines:** {", ".join(ENGINES[e]["display"] for e in ENGINE_IDS)}
- **Metric:** NOTPM = per-second commit rate x 60 x 0.45 (TPROC-C new-order mix is 45%)
- **BP iterations:** 64 VU, buffer pool 10-80 GiB in 10 GiB steps
- **VU iterations:** 50 GiB buffer pool, VU in {{1, 2, 4, 8, 16, 32, 64, 128}}

---

*Data source: [Percona-Lab-results/tpcc-benchmark-framework](https://github.com/Percona-Lab-results/tpcc-benchmark-framework)*
"""

md_out = "REPORT.md"
with open(md_out, "w", encoding="utf-8") as f:
    f.write(REPORT_MD)
print(f"Markdown report written -> {md_out}")
