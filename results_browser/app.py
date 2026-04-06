from flask import Flask, jsonify, render_template_string, request, send_from_directory
import os, json, csv, re
from pathlib import Path

app = Flask(__name__)
RESULTS_DIR = Path("/root/bench/results")

HTML = r"""<!DOCTYPE html>
<html>
<head>
<title>Benchmark Results Browser</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'SF Mono', 'Menlo', 'Consolas', monospace;
         background: #0f1117; color: #e0e0e0; padding: 20px; font-size: 13px; }
  h1 { text-align: center; margin-bottom: 16px; color: #7eb8da; font-size: 1.3em; }
  a { color: #7eb8da; text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Run list */
  .run-list { max-width: 1400px; margin: 0 auto; }
  .run-card { background: #1a1d27; border-radius: 8px; padding: 14px 18px; border: 1px solid #2a2d3a;
              margin-bottom: 8px; display: flex; align-items: center; gap: 16px; cursor: pointer; transition: border-color 0.2s; }
  .run-card:hover { border-color: #7eb8da; }
  .run-card.selected { border-color: #4ade80; background: #1e2433; }
  .run-label { font-weight: 600; color: #e0e0e0; min-width: 200px; }
  .run-meta { color: #888; font-size: 0.85em; display: flex; gap: 16px; flex-wrap: wrap; flex: 1; }
  .run-meta span { white-space: nowrap; }
  .tag { background: #2a2d3a; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }
  .tag-db { color: #a78bfa; }
  .tag-vu { color: #60a5fa; }
  .tag-dur { color: #fbbf24; }
  .tag-wh { color: #4ade80; }
  .tag-nopm { background: #1e3a2a; color: #4ade80; font-weight: 600; }
  .tag-tpm { background: #1e2a3a; color: #60a5fa; font-weight: 600; }

  /* Toolbar */
  .toolbar { max-width: 1400px; margin: 0 auto 12px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .toolbar input { background: #1a1d27; border: 1px solid #2a2d3a; color: #e0e0e0; padding: 6px 12px;
                    border-radius: 6px; font-family: inherit; font-size: 0.9em; width: 300px; }
  .btn { background: #2a2d3a; border: 1px solid #3a3d4a; color: #888; padding: 6px 14px;
         border-radius: 6px; cursor: pointer; font-family: inherit; font-size: 0.85em; }
  .btn:hover { background: #3a3d4a; color: #e0e0e0; }
  .btn-primary { background: #7eb8da; color: #0f1117; border-color: #7eb8da; }
  .btn-primary:hover { background: #5a9aba; }

  /* Detail view */
  .detail { max-width: 1400px; margin: 0 auto; display: none; }
  .detail.active { display: block; }
  .detail h2 { color: #7eb8da; font-size: 1em; margin: 16px 0 8px; text-transform: uppercase; letter-spacing: 1px; }
  .card { background: #1a1d27; border-radius: 8px; padding: 14px; border: 1px solid #2a2d3a; margin-bottom: 12px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; }
  .metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #1f222e; font-size: 0.9em; }
  .metric:last-child { border-bottom: none; }
  .mlabel { color: #888; }
  .mvalue { font-weight: 600; }
  .chart-box { position: relative; height: 280px; }
  canvas { width: 100% !important; height: 100% !important; }
  .file-list { display: flex; flex-wrap: wrap; gap: 6px; }
  .file-link { background: #2a2d3a; padding: 4px 10px; border-radius: 4px; font-size: 0.8em; color: #7eb8da; }
  .file-link:hover { background: #3a3d4a; }
  .pre-box { background: #1f222e; border-radius: 6px; padding: 10px; font-size: 0.8em;
             color: #888; max-height: 200px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }

  /* Compare */
  .compare-view { max-width: 1400px; margin: 0 auto; display: none; }
  .compare-view.active { display: block; }
  .cmp-table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
  .cmp-table th { text-align: left; color: #7eb8da; padding: 6px 10px; border-bottom: 1px solid #2a2d3a; }
  .cmp-table td { padding: 6px 10px; border-bottom: 1px solid #1f222e; }
  .cmp-table tr:hover td { background: #1e2433; }
  .cmp-better { color: #4ade80; }
  .cmp-worse { color: #ef4444; }
</style>
</head>
<body>
<h1>Benchmark Results Browser</h1>

<div class="toolbar">
  <input type="text" id="search" placeholder="Filter runs..." oninput="filterRuns()">
  <button class="btn" onclick="showList()">All Runs</button>
  <button class="btn btn-primary" id="compare-btn" onclick="compareSelected()" style="display:none;">Compare Selected</button>
  <span id="select-count" style="color:#888; font-size:0.85em;"></span>
</div>

<div class="run-list" id="run-list"></div>
<div class="detail" id="detail"></div>
<div class="compare-view" id="compare-view"></div>

<script>
let allRuns = [];
let selectedRuns = new Set();

function fmt(n) {
  if (n === null || n === undefined || n === '') return '-';
  n = Number(n);
  if (isNaN(n)) return '-';
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'G';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return n.toLocaleString();
}

function m(label, value) {
  return `<div class="metric"><span class="mlabel">${label}</span><span class="mvalue">${value}</span></div>`;
}

function showList() {
  document.getElementById('detail').className = 'detail';
  document.getElementById('compare-view').className = 'compare-view';
  document.getElementById('run-list').style.display = '';
  document.getElementById('search').style.display = '';
}

function updateSelectUI() {
  const n = selectedRuns.size;
  document.getElementById('compare-btn').style.display = n >= 2 ? '' : 'none';
  document.getElementById('select-count').textContent = n > 0 ? n + ' selected' : '';
  document.querySelectorAll('.run-card').forEach(el => {
    el.classList.toggle('selected', selectedRuns.has(el.dataset.dir));
  });
}

function toggleSelect(e, dir) {
  e.stopPropagation();
  if (selectedRuns.has(dir)) selectedRuns.delete(dir);
  else selectedRuns.add(dir);
  updateSelectUI();
}

async function loadRuns() {
  const r = await fetch('/api/runs');
  allRuns = await r.json();
  renderRuns(allRuns);
}

function filterRuns() {
  const q = document.getElementById('search').value.toLowerCase();
  renderRuns(allRuns.filter(r => r.dir.toLowerCase().includes(q) || (r.label||'').toLowerCase().includes(q)
    || (r.db_version||'').toLowerCase().includes(q)));
}

function renderRuns(runs) {
  const el = document.getElementById('run-list');
  if (runs.length === 0) { el.innerHTML = '<p style="color:#888;text-align:center;">No runs found.</p>'; return; }
  el.innerHTML = runs.map(r => {
    const nopm = r.nopm ? `<span class="tag tag-nopm">${fmt(r.nopm)} NOPM</span>` : '';
    const tpm = r.tpm ? `<span class="tag tag-tpm">${fmt(r.tpm)} TPM</span>` : '';
    return `<div class="run-card" data-dir="${r.dir}" onclick="openRun('${encodeURIComponent(r.dir)}')">
      <input type="checkbox" onclick="toggleSelect(event, '${r.dir}')" ${selectedRuns.has(r.dir)?'checked':''}>
      <div class="run-label">${r.label || r.dir.replace(/^run_\d+_\d+_/, '')}</div>
      <div class="run-meta">
        <span class="tag tag-db">${r.db_version || '?'}</span>
        <span class="tag tag-vu">${r.virtual_users || '?'} VU</span>
        <span class="tag tag-wh">${r.warehouses || '?'} WH</span>
        <span class="tag tag-dur">${r.duration_seconds || '?'}s</span>
        ${nopm} ${tpm}
        <span style="color:#555;">${r.timestamp || ''}</span>
      </div>
    </div>`;
  }).join('');
  updateSelectUI();
}

async function openRun(dir) {
  const r = await fetch('/api/run/' + dir);
  const d = await r.json();
  document.getElementById('run-list').style.display = 'none';
  document.getElementById('compare-view').className = 'compare-view';
  const el = document.getElementById('detail');
  el.className = 'detail active';

  let html = `<button class="btn" onclick="showList()" style="margin-bottom:12px;">&larr; Back</button>`;
  html += `<h2>${decodeURIComponent(dir)}</h2>`;

  // Params
  const p = d.params || {};
  html += '<div class="grid2"><div class="card"><h2>Configuration</h2>';
  html += m('Timestamp', p.timestamp || '-');
  html += m('Label', p.label || '-');
  html += m('DB Version', d.db_version || '-');
  html += m('Warehouses', p.warehouses || '-');
  html += m('Virtual Users', p.virtual_users || '-');
  html += m('Rampup', (p.rampup_seconds || '-') + 's');
  html += m('Duration', (p.duration_seconds || '-') + 's');
  html += m('HammerDB', p.hammerdb_version || '-');
  html += '</div>';

  // Results
  html += '<div class="card"><h2>Results</h2>';
  if (d.result) html += `<div class="pre-box">${d.result}</div>`;
  if (d.summary) html += `<div class="pre-box" style="margin-top:8px;">${d.summary}</div>`;
  html += '</div></div>';

  // QPS chart
  if (d.qps && d.qps.length > 1) {
    html += '<div class="card"><h2>QPS Over Time</h2><div class="chart-box"><canvas id="c-qps"></canvas></div></div>';
  }

  // NOPM chart
  if (d.nopm_samples && d.nopm_samples.length > 1) {
    html += '<div class="card"><h2>NOPM Samples (1s)</h2><div class="chart-box"><canvas id="c-nopm"></canvas></div></div>';
  }

  // Flushing & purge charts
  if (d.qps && d.qps.length > 1 && d.qps.some(r => r.pages_flushed_ps > 0)) {
    html += '<div class="card"><h2>Pages Flushed/s</h2><div class="chart-box"><canvas id="c-flush"></canvas></div></div>';
  }
  if (d.qps && d.qps.length > 1 && d.qps.some(r => r.purge_tps > 0)) {
    html += '<div class="card"><h2>Purge TPS</h2><div class="chart-box"><canvas id="c-purge"></canvas></div></div>';
  }
  if (d.qps && d.qps.length > 1 && d.qps.some(r => r.history_list_length > 0)) {
    html += '<div class="card"><h2>History List Length</h2><div class="chart-box"><canvas id="c-hll"></canvas></div></div>';
  }

  // Files
  html += '<div class="card"><h2>Files</h2><div class="file-list">';
  for (const f of (d.files || [])) {
    html += `<a class="file-link" href="/api/file/${dir}/${encodeURIComponent(f)}" target="_blank">${f}</a>`;
  }
  html += '</div></div>';

  el.innerHTML = html;

  // Draw charts
  if (d.qps && d.qps.length > 1) {
    drawTimeChart('c-qps', d.qps.map(r => r.qps), '#4ade80', 'QPS');
  }
  if (d.nopm_samples && d.nopm_samples.length > 1) {
    drawTimeChart('c-nopm', d.nopm_samples.map(r => r.nopm), '#a78bfa', 'NOPM');
  }
  if (d.qps && d.qps.length > 1) {
    drawTimeChart('c-flush', d.qps.map(r => r.pages_flushed_ps), '#f59e0b', 'Pages Flushed/s');
    drawTimeChart('c-purge', d.qps.map(r => r.purge_tps), '#4ade80', 'Purge TPS');
    drawTimeChart('c-hll', d.qps.map(r => r.history_list_length), '#a78bfa', 'History List Length');
  }
}

function drawTimeChart(canvasId, data, color, label) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 20, right: 10, bottom: 30, left: 60 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);

  const maxVal = Math.max(1, ...data) * 1.1;

  // Grid
  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.fillStyle = '#555'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + cH - (i / 4) * cH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillText(fmt(Math.round(maxVal * i / 4)), pad.left - 6, y + 4);
  }

  // Time labels
  ctx.textAlign = 'center'; ctx.fillStyle = '#555';
  const durSec = data.length;
  const durMin = Math.round(durSec / 60);
  ctx.fillText('0', pad.left, H - 8);
  ctx.fillText(durMin + 'm', pad.left + cW, H - 8);
  if (durMin > 2) {
    const mid = Math.round(durMin / 2);
    ctx.fillText(mid + 'm', pad.left + cW / 2, H - 8);
  }

  // Area
  const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  const rgb = color === '#4ade80' ? '74,222,128' : color === '#a78bfa' ? '167,139,250' : '96,165,250';
  grad.addColorStop(0, `rgba(${rgb},0.25)`); grad.addColorStop(1, `rgba(${rgb},0.0)`);
  ctx.beginPath(); ctx.moveTo(pad.left, pad.top + cH);
  for (let i = 0; i < data.length; i++) {
    ctx.lineTo(pad.left + (i / (data.length - 1)) * cW, pad.top + cH - (data[i] / maxVal) * cH);
  }
  ctx.lineTo(pad.left + cW, pad.top + cH); ctx.closePath(); ctx.fillStyle = grad; ctx.fill();

  // Dots (subsample if too many)
  const step = Math.max(1, Math.floor(data.length / 500));
  ctx.fillStyle = color;
  for (let i = 0; i < data.length; i += step) {
    const x = pad.left + (i / (data.length - 1)) * cW;
    const y = pad.top + cH - (data[i] / maxVal) * cH;
    ctx.beginPath(); ctx.arc(x, y, 1.5, 0, Math.PI * 2); ctx.fill();
  }

  // Label & stats
  const sum = data.reduce((a, b) => a + b, 0);
  const avg = Math.round(sum / data.length);
  const sorted = [...data].sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const mn = sorted[0], mx = sorted[sorted.length - 1];

  ctx.fillStyle = color; ctx.font = 'bold 12px SF Mono, Menlo, Consolas, monospace'; ctx.textAlign = 'left';
  ctx.fillText(`${label}  avg=${fmt(avg)}  min=${fmt(mn)}  p50=${fmt(p50)}  p95=${fmt(p95)}  max=${fmt(mx)}`, pad.left, pad.top - 6);
}

async function compareSelected() {
  if (selectedRuns.size < 2) return;
  const dirs = [...selectedRuns];
  const runs = await Promise.all(dirs.map(d => fetch('/api/run/' + encodeURIComponent(d)).then(r => r.json())));

  document.getElementById('run-list').style.display = 'none';
  document.getElementById('detail').className = 'detail';
  const el = document.getElementById('compare-view');
  el.className = 'compare-view active';

  let html = `<button class="btn" onclick="showList()" style="margin-bottom:12px;">&larr; Back</button>`;
  html += '<h2>Comparison</h2>';

  // Summary table
  html += '<div class="card"><table class="cmp-table"><tr><th>Metric</th>';
  for (const r of runs) {
    html += `<th>${(r.params && r.params.label) || r.dir}</th>`;
  }
  html += '</tr>';

  const rows = [
    ['DB Version', r => r.db_version || '-'],
    ['Warehouses', r => r.params?.warehouses || '-'],
    ['Virtual Users', r => r.params?.virtual_users || '-'],
    ['Duration', r => (r.params?.duration_seconds || '-') + 's'],
  ];

  // QPS stats
  const qpsStats = runs.map(r => {
    if (!r.qps || r.qps.length < 2) return {};
    const vals = r.qps.map(x => x.qps).filter(v => v > 0);
    if (!vals.length) return {};
    const sum = vals.reduce((a, b) => a + b, 0);
    const sorted = [...vals].sort((a, b) => a - b);
    return { avg: Math.round(sum / vals.length), p50: sorted[Math.floor(sorted.length * 0.5)],
             p95: sorted[Math.floor(sorted.length * 0.95)], max: sorted[sorted.length - 1] };
  });

  const flushStats = runs.map(r => {
    if (!r.qps || r.qps.length < 2) return {};
    const vals = r.qps.map(x => x.pages_flushed_ps).filter(v => v > 0);
    if (!vals.length) return {};
    const sum = vals.reduce((a, b) => a + b, 0);
    const sorted = [...vals].sort((a, b) => a - b);
    return { avg: Math.round(sum / vals.length), p95: sorted[Math.floor(sorted.length * 0.95)], max: sorted[sorted.length - 1] };
  });

  const purgeStats = runs.map(r => {
    if (!r.qps || r.qps.length < 2) return {};
    const vals = r.qps.map(x => x.purge_tps).filter(v => v > 0);
    if (!vals.length) return {};
    const sum = vals.reduce((a, b) => a + b, 0);
    const sorted = [...vals].sort((a, b) => a - b);
    return { avg: Math.round(sum / vals.length), p95: sorted[Math.floor(sorted.length * 0.95)], max: sorted[sorted.length - 1] };
  });

  rows.push(['Avg QPS', (r, i) => fmt(qpsStats[i]?.avg)]);
  rows.push(['P95 QPS', (r, i) => fmt(qpsStats[i]?.p95)]);
  rows.push(['Max QPS', (r, i) => fmt(qpsStats[i]?.max)]);
  rows.push(['Avg Flush/s', (r, i) => fmt(flushStats[i]?.avg)]);
  rows.push(['P95 Flush/s', (r, i) => fmt(flushStats[i]?.p95)]);
  rows.push(['Avg Purge TPS', (r, i) => fmt(purgeStats[i]?.avg)]);
  rows.push(['P95 Purge TPS', (r, i) => fmt(purgeStats[i]?.p95)]);

  // Find best values for highlighting
  for (const row of rows) {
    html += `<tr><td>${row[0]}</td>`;
    const vals = runs.map((r, i) => row[1](r, i));
    const numVals = vals.map(v => parseFloat(String(v).replace(/[,KMG]/g, '')));
    const maxNum = Math.max(...numVals.filter(v => !isNaN(v)));
    for (let i = 0; i < runs.length; i++) {
      const isRate = row[0].includes('QPS') || row[0].includes('Purge');
      const cls = (!isNaN(numVals[i]) && numVals[i] === maxNum && isRate)
        ? ' class="cmp-better"' : '';
      html += `<td${cls}>${vals[i]}</td>`;
    }
    html += '</tr>';
  }

  html += '</table></div>';

  // Overlay charts
  html += '<div class="card"><h2>QPS Comparison</h2><div class="chart-box"><canvas id="c-cmp-qps"></canvas></div></div>';
  html += '<div class="card"><h2>Pages Flushed/s Comparison</h2><div class="chart-box"><canvas id="c-cmp-flush"></canvas></div></div>';
  html += '<div class="card"><h2>Purge TPS Comparison</h2><div class="chart-box"><canvas id="c-cmp-purge"></canvas></div></div>';
  html += '<div class="card"><h2>History List Length Comparison</h2><div class="chart-box"><canvas id="c-cmp-hll"></canvas></div></div>';

  el.innerHTML = html;

  const colors = ['#4ade80', '#60a5fa', '#a78bfa', '#f59e0b', '#ef4444', '#ec4899'];
  const mkSeries = (field) => runs.map((r, i) => ({
    data: (r.qps || []).map(x => x[field]),
    color: colors[i % colors.length],
    label: (r.params && r.params.label) || r.dir
  }));
  drawCompareChart('c-cmp-qps', mkSeries('qps'));
  drawCompareChart('c-cmp-flush', mkSeries('pages_flushed_ps'));
  drawCompareChart('c-cmp-purge', mkSeries('purge_tps'));
  drawCompareChart('c-cmp-hll', mkSeries('history_list_length'));
}

function drawCompareChart(canvasId, series) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 20, right: 10, bottom: 30, left: 60 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;

  const allMax = Math.max(1, ...series.flatMap(s => s.data)) * 1.1;
  const maxLen = Math.max(...series.map(s => s.data.length));

  // Grid
  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.fillStyle = '#555'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + cH - (i / 4) * cH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillText(fmt(Math.round(allMax * i / 4)), pad.left - 6, y + 4);
  }

  // Draw each series
  for (const s of series) {
    if (s.data.length < 2) continue;
    const step = Math.max(1, Math.floor(s.data.length / 600));
    ctx.fillStyle = s.color;
    for (let i = 0; i < s.data.length; i += step) {
      const x = pad.left + (i / (maxLen - 1)) * cW;
      const y = pad.top + cH - (s.data[i] / allMax) * cH;
      ctx.beginPath(); ctx.arc(x, y, 1.5, 0, Math.PI * 2); ctx.fill();
    }
  }

  // Legend
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.textAlign = 'left';
  let lx = pad.left;
  for (const s of series) {
    ctx.fillStyle = s.color;
    ctx.fillText('● ' + s.label, lx, pad.top - 6);
    lx += ctx.measureText('● ' + s.label).width + 20;
  }
}

loadRuns();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/runs")
def list_runs():
    runs = []
    if not RESULTS_DIR.exists():
        return jsonify(runs)
    for d in sorted(RESULTS_DIR.iterdir(), reverse=True):
        if not d.is_dir() or not d.name.startswith("run_"):
            continue
        info = {"dir": d.name}

        # Load params
        params_file = d / "bench_params.json"
        if params_file.exists():
            try:
                params = json.loads(params_file.read_text())
                info.update({
                    "label": params.get("label", ""),
                    "timestamp": params.get("timestamp", ""),
                    "warehouses": params.get("warehouses"),
                    "virtual_users": params.get("virtual_users"),
                    "duration_seconds": params.get("duration_seconds"),
                })
            except Exception:
                pass

        # DB version
        ver_file = d / "mariadb_version.txt"
        if ver_file.exists():
            info["db_version"] = ver_file.read_text().strip()

        # Extract NOPM/TPM from result or hammerdb log
        result_file = d / "result.txt"
        if result_file.exists():
            txt = result_file.read_text()
            nopm_m = re.search(r"(\d+)\s+NOPM", txt)
            tpm_m = re.search(r"(\d+)\s+TPM", txt)
            if nopm_m:
                info["nopm"] = int(nopm_m.group(1))
            if tpm_m:
                info["tpm"] = int(tpm_m.group(1))
        else:
            log_file = d / "hammerdb.log"
            if log_file.exists():
                try:
                    txt = log_file.read_text()
                    m = re.search(r"System achieved (\d+) NOPM from (\d+) .*TPM", txt)
                    if m:
                        info["nopm"] = int(m.group(1))
                        info["tpm"] = int(m.group(2))
                except Exception:
                    pass

        runs.append(info)
    return jsonify(runs)


@app.route("/api/run/<path:dirname>")
def get_run(dirname):
    d = RESULTS_DIR / dirname
    if not d.exists():
        return jsonify({"error": "not found"}), 404

    result = {}

    # Params
    params_file = d / "bench_params.json"
    if params_file.exists():
        try:
            result["params"] = json.loads(params_file.read_text())
        except Exception:
            pass

    # DB version
    ver_file = d / "mariadb_version.txt"
    if ver_file.exists():
        result["db_version"] = ver_file.read_text().strip()

    # Result text
    result_file = d / "result.txt"
    if result_file.exists():
        result["result"] = result_file.read_text().strip()

    # Summary
    summary_file = d / "summary.txt"
    if summary_file.exists():
        result["summary"] = summary_file.read_text().strip()

    # QPS CSV
    qps_file = d / "qps.csv"
    if qps_file.exists():
        try:
            with open(qps_file) as f:
                reader = csv.DictReader(f)
                result["qps"] = [
                    {"qps": int(row.get("qps", 0)), "tps": int(row.get("tps", 0)),
                     "threads_running": int(row.get("threads_running", 0)),
                     "pages_flushed_ps": int(row.get("pages_flushed_ps", 0)),
                     "purge_tps": int(row.get("purge_tps", 0)),
                     "history_list_length": int(row.get("history_list_length", 0))}
                    for row in reader
                ]
        except Exception:
            pass

    # NOPM samples
    nopm_file = d / "nopm_samples.csv"
    if nopm_file.exists():
        try:
            with open(nopm_file) as f:
                reader = csv.DictReader(f)
                result["nopm_samples"] = [
                    {"nopm": int(row.get("nopm", 0)), "tpm": int(row.get("tpm", 0))}
                    for row in reader
                ]
        except Exception:
            pass

    # File list
    result["files"] = sorted([f.name for f in d.iterdir() if f.is_file()])
    result["dir"] = dirname

    return jsonify(result)


@app.route("/api/file/<path:filepath>")
def get_file(filepath):
    parts = filepath.split("/", 1)
    if len(parts) != 2:
        return "not found", 404
    dirname, filename = parts
    d = RESULTS_DIR / dirname
    if not d.exists() or not (d / filename).exists():
        return "not found", 404
    return send_from_directory(str(d), filename)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)
