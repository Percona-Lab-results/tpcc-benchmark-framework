"""
Extract benchmark data from git repo (handles Windows paths-with-spaces issue).
Outputs: data/runs.json  — array of run objects with params + qps timeseries
"""
import subprocess, json, csv, io, sys, os

REPO = os.path.dirname(os.path.abspath(__file__))


def git_show(path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"HEAD:{path}"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def list_runs() -> list[str]:
    result = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "HEAD"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    dirs = set()
    for line in result.stdout.splitlines():
        if line.startswith("results/") and line.endswith("/bench_params.json"):
            run_dir = line[: -len("/bench_params.json")]
            dirs.add(run_dir)
    return sorted(dirs)


def parse_qps_csv(content: str) -> list[dict]:
    rows = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        rows.append({k: v for k, v in row.items()})
    return rows


def get_db_version(run_dir: str) -> str:
    for fname in ("mariadb_version.txt", "mysql_version.txt"):
        v = git_show(f"{run_dir}/{fname}")
        if v:
            return v.strip()
    return None


def infer_db(label: str, version: str) -> str:
    """Determine database engine from label/version."""
    label_lower = label.lower() if label else ""
    ver_lower = (version or "").lower()
    if "percona" in label_lower or label_lower.startswith("ps "):
        return "Percona Server"
    if "mysql" in label_lower:
        return "MySQL"
    if "mariadb" in label_lower:
        return "MariaDB"
    # fallback to version string
    if "percona" in ver_lower:
        return "Percona Server"
    if "mariadb" in ver_lower:
        return "MariaDB"
    if "mysql" in ver_lower:
        return "MySQL"
    return "unknown"


def summarize_tps(rows: list[dict], rampup_seconds: int) -> dict:
    """Compute steady-state TPS stats (excluding rampup)."""
    tps_values = []
    if not rows:
        return {}
    # parse timestamp to skip rampup by row count approximation
    # qps.csv timestamps: use rampup_seconds to skip first N seconds
    if rows and "timestamp" in rows[0]:
        from datetime import datetime
        try:
            t0 = datetime.fromisoformat(rows[0]["timestamp"])
            steady = [
                float(r["tps"])
                for r in rows
                if r.get("tps")
                and (datetime.fromisoformat(r["timestamp"]) - t0).total_seconds()
                >= rampup_seconds
            ]
            tps_values = steady
        except Exception:
            pass
    if not tps_values:
        tps_values = [float(r["tps"]) for r in rows if r.get("tps")]

    if not tps_values:
        return {}
    return {
        "avg": round(sum(tps_values) / len(tps_values), 1),
        "max": round(max(tps_values), 1),
        "min": round(min(tps_values), 1),
        "p95": round(sorted(tps_values)[int(len(tps_values) * 0.95)], 1),
    }


def main():
    os.makedirs("data", exist_ok=True)
    runs = []
    run_dirs = list_runs()
    print(f"Found {len(run_dirs)} runs", file=sys.stderr)

    for run_dir in run_dirs:
        run_name = run_dir.split("/", 1)[1]  # strip "results/"
        params_raw = git_show(f"{run_dir}/bench_params.json")
        if not params_raw:
            print(f"  SKIP {run_name} — no bench_params.json", file=sys.stderr)
            continue
        params = json.loads(params_raw)

        qps_raw = git_show(f"{run_dir}/qps.csv")
        qps_rows = parse_qps_csv(qps_raw) if qps_raw else []

        version = get_db_version(run_dir)
        label = params.get("label", run_name)
        db = infer_db(label, version)

        tps_stats = summarize_tps(qps_rows, params.get("rampup_seconds", 60))

        run = {
            "run_name": run_name,
            "timestamp": params.get("timestamp"),
            "label": label,
            "db": db,
            "version": version,
            "virtual_users": params.get("virtual_users"),
            "warehouses": params.get("warehouses"),
            "duration_seconds": params.get("duration_seconds"),
            "rampup_seconds": params.get("rampup_seconds"),
            "tps": tps_stats,
            "qps": qps_rows,
        }
        runs.append(run)
        print(f"  OK  {run_name}  db={db}  avg_tps={tps_stats.get('avg', '?')}", file=sys.stderr)

    out_path = os.path.join(os.path.dirname(__file__), "data", "runs.json")
    with open(out_path, "w") as f:
        json.dump(runs, f, indent=2)
    print(f"\nWrote {len(runs)} runs → {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
