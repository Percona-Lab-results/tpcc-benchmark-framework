from flask import Flask, jsonify, render_template_string
import psutil
import mysql.connector
import time

app = Flask(__name__)

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "rootpassword",
}

HTML = r"""<!DOCTYPE html>
<html>
<head>
<title>System & MySQL Monitor</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'SF Mono', 'Menlo', 'Consolas', monospace;
         background: #0f1117; color: #e0e0e0; padding: 16px; font-size: 13px; }
  h1 { text-align: center; margin-bottom: 16px; color: #7eb8da; font-size: 1.3em; font-weight: 600; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; max-width: 1400px; margin: 0 auto; }
  .card { background: #1a1d27; border-radius: 8px; padding: 14px; border: 1px solid #2a2d3a; }
  .card h2 { font-size: 0.8em; color: #7eb8da; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1.5px; font-weight: 600; }
  .metric { display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #1f222e; font-size: 0.9em; }
  .metric:last-child { border-bottom: none; }
  .label { color: #888; }
  .value { font-weight: 600; font-variant-numeric: tabular-nums; }
  .val-green { color: #4ade80; }
  .val-yellow { color: #fbbf24; }
  .val-red { color: #ef4444; }
  .val-blue { color: #60a5fa; }
  .bar-bg { background: #2a2d3a; border-radius: 4px; height: 6px; margin-top: 3px; }
  .bar { height: 6px; border-radius: 4px; transition: width 0.5s; }
  .bar-cpu { background: linear-gradient(90deg, #4ade80, #f59e0b, #ef4444); }
  .bar-mem { background: linear-gradient(90deg, #60a5fa, #a78bfa); }
  .bar-disk { background: linear-gradient(90deg, #34d399, #fbbf24); }
  .status { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .status-ok { background: #4ade80; }
  .status-err { background: #ef4444; }
  .wide { grid-column: 1 / -1; }
  .mysql-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; }
  .mysql-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
  #updated { text-align: center; color: #555; font-size: 0.7em; margin-top: 10px; }
  .chart-container { position: relative; height: 320px; }
  .chart-container-hist { position: relative; height: 220px; }
  canvas { width: 100% !important; height: 100% !important; }
  .qps-current { position: absolute; top: 6px; right: 10px; font-size: 1.3em; font-weight: 700; color: #4ade80; font-variant-numeric: tabular-nums; }

  /* Processlist table */
  .proctable { width: 100%; border-collapse: collapse; font-size: 0.85em; }
  .proctable th { text-align: left; color: #7eb8da; font-weight: 600; padding: 4px 8px;
                   border-bottom: 1px solid #2a2d3a; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.5px; }
  .proctable td { padding: 3px 8px; border-bottom: 1px solid #1f222e; white-space: nowrap;
                   overflow: hidden; text-overflow: ellipsis; max-width: 400px; }
  .proctable tr:hover td { background: #22253a; }
  .proctable .time-warn { color: #fbbf24; }
  .proctable .time-crit { color: #ef4444; font-weight: 700; }
  .proctable .state { color: #a78bfa; }
  .proctable .query { color: #888; font-size: 0.9em; }

  /* Deadlock box */
  .period-btn { background: #2a2d3a; border: 1px solid #3a3d4a; color: #888; padding: 2px 10px;
                 border-radius: 4px; cursor: pointer; font-size: 0.8em; font-family: inherit; }
  .period-btn:hover { background: #3a3d4a; color: #e0e0e0; }
  .period-btn.active { background: #7eb8da; color: #0f1117; border-color: #7eb8da; }
  .deadlock-box { background: #1f222e; border-radius: 6px; padding: 10px; font-size: 0.8em;
                   color: #888; max-height: 150px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }
</style>
</head>
<body>
<h1>System & MySQL Monitor</h1>
<div class="grid">
  <div class="card">
    <h2>CPU</h2>
    <div id="cpu"></div>
  </div>
  <div class="card">
    <h2>Memory</h2>
    <div id="mem"></div>
  </div>
  <div class="card">
    <h2>Disk (/data)</h2>
    <div id="disk"></div>
  </div>
  <div class="card">
    <h2>Network</h2>
    <div id="net"></div>
  </div>

  <div class="card wide">
    <h2>Queries Per Second</h2>
    <div class="chart-container">
      <canvas id="qps-chart"></canvas>
      <div class="qps-current" id="qps-current">-- QPS</div>
    </div>
  </div>

  <div class="card wide">
    <h2 style="display:flex; align-items:center; gap:12px;">
      QPS Distribution
      <span style="display:flex; gap:4px;" id="hist-period-btns">
        <button class="period-btn" data-secs="60">1m</button>
        <button class="period-btn" data-secs="120">2m</button>
        <button class="period-btn" data-secs="300">5m</button>
        <button class="period-btn active" data-secs="900">15m</button>
        <button class="period-btn" data-secs="0">All</button>
      </span>
      <span id="hist-period-label" style="font-size:0.75em; color:#555; font-weight:400;"></span>
    </h2>
    <div class="chart-container-hist">
      <canvas id="qps-hist"></canvas>
    </div>
  </div>

  <div class="card wide">
    <h2><span class="status" id="mysql-status"></span>MySQL Dashboard</h2>
    <div class="mysql-grid" id="mysql"></div>
  </div>

  <div class="card wide">
    <h2>InnoDB History List Length</h2>
    <div class="chart-container" style="height:200px;">
      <canvas id="hll-chart"></canvas>
      <div class="qps-current" id="hll-current" style="color:#a78bfa;">--</div>
    </div>
  </div>

  <div class="card wide">
    <h2>InnoDB Flushing Activity</h2>
    <div class="chart-container" style="height:240px;">
      <canvas id="flush-chart"></canvas>
    </div>
    <div id="flush-legend" style="display:flex; gap:16px; justify-content:center; margin-top:6px; font-size:0.8em;"></div>
  </div>

  <div class="card">
    <h2>InnoDB Transactions</h2>
    <div id="innodb-txn"></div>
  </div>
  <div class="card">
    <h2>InnoDB I/O</h2>
    <div id="innodb-io"></div>
  </div>

  <div class="card">
    <h2>InnoDB Buffer Pool</h2>
    <div id="innodb-bp"></div>
  </div>
  <div class="card">
    <h2>InnoDB Redo Log</h2>
    <div id="innodb-log"></div>
    <div style="height:100px; margin-top:8px;">
      <canvas id="log-gauge"></canvas>
    </div>
  </div>

  <div class="card">
    <h2>InnoDB Row Operations</h2>
    <div id="innodb-rows"></div>
  </div>

  <div class="card">
    <h2>Command Counters</h2>
    <div id="cmd-counters"></div>
  </div>
  <div class="card">
    <h2>Locks & Waits</h2>
    <div id="locks"></div>
  </div>

  <div class="card wide">
    <h2>Processlist (active queries)</h2>
    <div id="processlist" style="overflow-x:auto;"></div>
  </div>

  <div class="card wide">
    <h2>Latest Detected Deadlock</h2>
    <div class="deadlock-box" id="deadlock">No deadlock info</div>
  </div>
</div>
<div id="updated"></div>

<script>
function m(label, value, cls) {
  const vc = cls ? ` ${cls}` : '';
  return `<div class="metric"><span class="label">${label}</span><span class="value${vc}">${value}</span></div>`;
}
function bar(pct, cls) {
  return `<div class="bar-bg"><div class="bar ${cls}" style="width:${Math.min(pct,100)}%"></div></div>`;
}
function fmt(n) {
  if (n === null || n === undefined) return '0';
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'G';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return n.toString();
}
function fmtBytes(b) {
  if (b >= 1073741824) return (b/1073741824).toFixed(1) + ' GB';
  if (b >= 1048576) return (b/1048576).toFixed(1) + ' MB';
  if (b >= 1024) return (b/1024).toFixed(1) + ' KB';
  return b + ' B';
}
function fmtSec(s) {
  if (s >= 86400) return Math.floor(s/86400) + 'd ' + Math.floor((s%86400)/3600) + 'h';
  if (s >= 3600) return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
  if (s >= 60) return Math.floor(s/60) + 'm ' + (s%60) + 's';
  return s + 's';
}
function escHtml(s) {
  const d = document.createElement('div'); d.textContent = s; return d.innerHTML;
}

// QPS chart state — 450 points = 15 min at 2s intervals
const MAX_POINTS = 450;
const qpsData = [];
let prevStats = null;
let prevTime = null;
const HIST_BUCKETS = 20;
let histPeriodSecs = 900; // default 15 min

// History list length chart — same 2s interval as QPS
const hllData = [];

// Flushing activity chart
const flushSeries = {
  flushed_ps: [],   // pages flushed per second
  dirty_pct: [],    // dirty pages as % of total
  log_fill_pct: [], // redo log fill %
  log_fsyncs_ps: [], // log fsyncs per second
  purge_tps: []     // purge transactions per second
};


function drawChart() {
  const canvas = document.getElementById('qps-chart');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 10, right: 10, bottom: 25, left: 55 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);
  if (qpsData.length < 2) return;
  const maxQps = Math.max(100, ...qpsData) * 1.1;

  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.fillStyle = '#555'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + cH - (i / 4) * cH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillText(fmt(Math.round(maxQps * i / 4)), pad.left - 6, y + 4);
  }
  ctx.textAlign = 'center'; ctx.fillStyle = '#555';
  ctx.fillText(`-${qpsData.length * 2}s`, pad.left, H - 4);
  ctx.fillText('now', pad.left + cW, H - 4);

  const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  grad.addColorStop(0, 'rgba(74, 222, 128, 0.3)'); grad.addColorStop(1, 'rgba(74, 222, 128, 0.0)');
  ctx.beginPath(); ctx.moveTo(pad.left, pad.top + cH);
  for (let i = 0; i < qpsData.length; i++) {
    ctx.lineTo(pad.left + (i / (MAX_POINTS - 1)) * cW, pad.top + cH - (qpsData[i] / maxQps) * cH);
  }
  ctx.lineTo(pad.left + ((qpsData.length - 1) / (MAX_POINTS - 1)) * cW, pad.top + cH);
  ctx.closePath(); ctx.fillStyle = grad; ctx.fill();

  ctx.fillStyle = '#4ade80';
  for (let i = 0; i < qpsData.length; i++) {
    const x = pad.left + (i / (MAX_POINTS - 1)) * cW;
    const y = pad.top + cH - (qpsData[i] / maxQps) * cH;
    ctx.beginPath();
    ctx.arc(x, y, 2.5, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawHllChart() {
  const canvas = document.getElementById('hll-chart');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 10, right: 10, bottom: 25, left: 55 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);
  if (hllData.length < 2) return;
  const maxVal = Math.max(1000, ...hllData) * 1.1;

  // Grid
  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.fillStyle = '#555'; ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + cH - (i / 4) * cH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillText(fmt(Math.round(maxVal * i / 4)), pad.left - 6, y + 4);
  }
  ctx.textAlign = 'center'; ctx.fillStyle = '#555';
  ctx.fillText(`-${hllData.length * 2}s`, pad.left, H - 4);
  ctx.fillText('now', pad.left + cW, H - 4);

  // Area fill
  const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
  grad.addColorStop(0, 'rgba(167, 139, 250, 0.3)'); grad.addColorStop(1, 'rgba(167, 139, 250, 0.0)');
  ctx.beginPath(); ctx.moveTo(pad.left, pad.top + cH);
  for (let i = 0; i < hllData.length; i++) {
    ctx.lineTo(pad.left + (i / (MAX_POINTS - 1)) * cW, pad.top + cH - (hllData[i] / maxVal) * cH);
  }
  ctx.lineTo(pad.left + ((hllData.length - 1) / (MAX_POINTS - 1)) * cW, pad.top + cH);
  ctx.closePath(); ctx.fillStyle = grad; ctx.fill();

  // Dots
  for (let i = 0; i < hllData.length; i++) {
    const x = pad.left + (i / (MAX_POINTS - 1)) * cW;
    const y = pad.top + cH - (hllData[i] / maxVal) * cH;
    const v = hllData[i];
    ctx.fillStyle = v >= 100000 ? '#ef4444' : v >= 10000 ? '#fbbf24' : '#a78bfa';
    ctx.beginPath(); ctx.arc(x, y, 2.5, 0, Math.PI * 2); ctx.fill();
  }

  // Threshold lines
  for (const [label, val, color] of [['10K', 10000, '#fbbf24'], ['100K', 100000, '#ef4444']]) {
    if (val < maxVal) {
      const y = pad.top + cH - (val / maxVal) * cH;
      ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = color; ctx.font = '10px SF Mono, Menlo, Consolas, monospace';
      ctx.textAlign = 'left'; ctx.fillText(label, pad.left + 4, y - 4);
    }
  }
}

function drawFlushChart() {
  const canvas = document.getElementById('flush-chart');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 10, right: 55, bottom: 25, left: 55 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);

  const n = flushSeries.flushed_ps.length;
  if (n < 2) return;

  // Left axis: pages flushed/s (and log fsyncs/s)
  const maxFlush = Math.max(100, ...flushSeries.flushed_ps, ...flushSeries.log_fsyncs_ps) * 1.1;
  // Right axis: percentage (dirty %, log fill %) — always 0-100

  // Left axis grid & labels
  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  ctx.font = '10px SF Mono, Menlo, Consolas, monospace';
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + cH - (i / 4) * cH;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
    ctx.fillStyle = '#555'; ctx.textAlign = 'right';
    ctx.fillText(fmt(Math.round(maxFlush * i / 4)), pad.left - 6, y + 4);
    ctx.fillStyle = '#555'; ctx.textAlign = 'left';
    ctx.fillText(Math.round(100 * i / 4) + '%', pad.left + cW + 6, y + 4);
  }

  // Time labels
  ctx.textAlign = 'center'; ctx.fillStyle = '#555';
  ctx.fillText(`-${n * 2}s`, pad.left, H - 4);
  ctx.fillText('now', pad.left + cW, H - 4);

  const series = [
    { data: flushSeries.flushed_ps, color: '#f59e0b', axis: 'left', label: 'Pages Flushed/s' },
    { data: flushSeries.log_fsyncs_ps, color: '#60a5fa', axis: 'left', label: 'Log Fsyncs/s' },
    { data: flushSeries.purge_tps, color: '#4ade80', axis: 'left', label: 'Purge TPS' },
    { data: flushSeries.dirty_pct, color: '#ef4444', axis: 'right', label: 'Dirty Pages %' },
    { data: flushSeries.log_fill_pct, color: '#a78bfa', axis: 'right', label: 'Redo Log Fill %' },
  ];

  for (const s of series) {
    const max = s.axis === 'left' ? maxFlush : 100;
    ctx.fillStyle = s.color;
    for (let i = 0; i < s.data.length; i++) {
      const x = pad.left + (i / (MAX_POINTS - 1)) * cW;
      const y = pad.top + cH - (s.data[i] / max) * cH;
      ctx.beginPath(); ctx.arc(x, y, 2, 0, Math.PI * 2); ctx.fill();
    }
  }

  // Legend
  document.getElementById('flush-legend').innerHTML = series.map(s =>
    `<span style="color:${s.color};">● ${s.label}</span>`
  ).join('');
}

function drawHistogram() {
  // Slice data to the selected period (each point = 2s)
  const maxPoints = histPeriodSecs > 0 ? Math.ceil(histPeriodSecs / 2) : qpsData.length;
  const data = qpsData.slice(-maxPoints);
  const periodLabel = histPeriodSecs > 0 ? (histPeriodSecs >= 60 ? (histPeriodSecs/60)+'m' : histPeriodSecs+'s') : 'all';
  document.getElementById('hist-period-label').textContent = `(${data.length} samples, last ${periodLabel})`;

  const canvas = document.getElementById('qps-hist');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const pad = { top: 10, right: 10, bottom: 40, left: 55 };
  const cW = W - pad.left - pad.right;
  const cH = H - pad.top - pad.bottom;
  ctx.clearRect(0, 0, W, H);

  if (data.length < 3) {
    ctx.fillStyle = '#555'; ctx.font = '12px SF Mono, Menlo, Consolas, monospace';
    ctx.textAlign = 'center'; ctx.fillText('Collecting data...', W/2, H/2);
    return;
  }

  const minQ = Math.min(...data);
  const maxQ = Math.max(...data);

  // Stats
  const sum = data.reduce((a,b) => a+b, 0);
  const avg = sum / data.length;
  const sorted = [...data].sort((a,b) => a-b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.floor(sorted.length * 0.95)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];

  if (maxQ === minQ) {
    ctx.fillStyle = '#555'; ctx.font = '12px SF Mono, Menlo, Consolas, monospace';
    ctx.textAlign = 'center'; ctx.fillText('All values equal: ' + fmt(minQ) + ' QPS', W/2, H/2);
    return;
  }

  // Build buckets
  const bucketWidth = (maxQ - minQ) / HIST_BUCKETS;
  const buckets = new Array(HIST_BUCKETS).fill(0);
  for (const v of data) {
    let idx = Math.floor((v - minQ) / bucketWidth);
    if (idx >= HIST_BUCKETS) idx = HIST_BUCKETS - 1;
    buckets[idx]++;
  }
  const maxCount = Math.max(...buckets);

  // Draw bars
  const barGap = 2;
  const barW = (cW / HIST_BUCKETS) - barGap;
  for (let i = 0; i < HIST_BUCKETS; i++) {
    const barH = maxCount > 0 ? (buckets[i] / maxCount) * cH : 0;
    const x = pad.left + i * (barW + barGap);
    const y = pad.top + cH - barH;

    // Color based on position: green for low QPS buckets, yellow mid, red high
    const t = i / (HIST_BUCKETS - 1);
    const grad = ctx.createLinearGradient(x, y, x, pad.top + cH);
    grad.addColorStop(0, `rgba(96, 165, 250, 0.9)`);
    grad.addColorStop(1, `rgba(96, 165, 250, 0.4)`);
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.roundRect(x, y, barW, barH, [3, 3, 0, 0]);
    ctx.fill();

    // Count label on top of bar if nonzero
    if (buckets[i] > 0 && barH > 14) {
      ctx.fillStyle = '#e0e0e0'; ctx.font = '10px SF Mono, Menlo, Consolas, monospace';
      ctx.textAlign = 'center';
      ctx.fillText(buckets[i], x + barW/2, y - 3);
    }
  }

  // X-axis labels (bucket ranges)
  ctx.fillStyle = '#555'; ctx.font = '10px SF Mono, Menlo, Consolas, monospace'; ctx.textAlign = 'center';
  const labelEvery = Math.max(1, Math.floor(HIST_BUCKETS / 8));
  for (let i = 0; i < HIST_BUCKETS; i += labelEvery) {
    const val = minQ + i * bucketWidth;
    const x = pad.left + i * (barW + barGap) + barW/2;
    ctx.fillText(fmt(Math.round(val)), x, pad.top + cH + 14);
  }
  // Last label
  ctx.fillText(fmt(Math.round(maxQ)), pad.left + (HIST_BUCKETS - 1) * (barW + barGap) + barW/2, pad.top + cH + 14);

  // Y-axis labels
  ctx.textAlign = 'right'; ctx.fillStyle = '#555';
  for (let i = 0; i <= 3; i++) {
    const val = Math.round(maxCount * i / 3);
    const y = pad.top + cH - (i / 3) * cH;
    ctx.fillText(val, pad.left - 6, y + 4);
    ctx.strokeStyle = '#1f222e'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + cW, y); ctx.stroke();
  }

  // Stats line
  ctx.font = '11px SF Mono, Menlo, Consolas, monospace'; ctx.textAlign = 'left';
  const statsY = pad.top + cH + 28;
  const stats = `n=${data.length}  avg=${fmt(Math.round(avg))}  min=${fmt(minQ)}  p50=${fmt(p50)}  p95=${fmt(p95)}  p99=${fmt(p99)}  max=${fmt(maxQ)}`;
  ctx.fillStyle = '#7eb8da';
  ctx.fillText(stats, pad.left, statsY);

  // Draw percentile markers on the chart
  for (const [label, val, color] of [['p50', p50, '#4ade80'], ['p95', p95, '#fbbf24'], ['p99', p99, '#ef4444']]) {
    const xPos = pad.left + ((val - minQ) / (maxQ - minQ)) * cW;
    if (xPos >= pad.left && xPos <= pad.left + cW) {
      ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(xPos, pad.top); ctx.lineTo(xPos, pad.top + cH); ctx.stroke();
      ctx.setLineDash([]);
    }
  }
}

function drawLogGauge(pct) {
  const canvas = document.getElementById('log-gauge');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const W = rect.width, H = rect.height;
  const cx = W / 2, cy = H - 10;
  const radius = Math.min(W / 2 - 10, H - 20);
  const startAngle = Math.PI;
  const endAngle = 2 * Math.PI;
  ctx.clearRect(0, 0, W, H);

  // Background arc
  ctx.beginPath(); ctx.arc(cx, cy, radius, startAngle, endAngle);
  ctx.lineWidth = 18; ctx.strokeStyle = '#2a2d3a'; ctx.lineCap = 'round'; ctx.stroke();

  // Filled arc
  const fillAngle = startAngle + (pct / 100) * Math.PI;
  const grad = ctx.createLinearGradient(cx - radius, cy, cx + radius, cy);
  if (pct < 70) {
    grad.addColorStop(0, '#4ade80'); grad.addColorStop(1, '#60a5fa');
  } else if (pct < 90) {
    grad.addColorStop(0, '#fbbf24'); grad.addColorStop(1, '#f59e0b');
  } else {
    grad.addColorStop(0, '#ef4444'); grad.addColorStop(1, '#dc2626');
  }
  ctx.beginPath(); ctx.arc(cx, cy, radius, startAngle, fillAngle);
  ctx.lineWidth = 18; ctx.strokeStyle = grad; ctx.lineCap = 'round'; ctx.stroke();

  // Percentage text
  ctx.fillStyle = pct >= 90 ? '#ef4444' : pct >= 70 ? '#fbbf24' : '#4ade80';
  ctx.font = 'bold 22px SF Mono, Menlo, Consolas, monospace';
  ctx.textAlign = 'center'; ctx.textBaseline = 'bottom';
  ctx.fillText(pct.toFixed(1) + '%', cx, cy - 8);

  // Label
  ctx.fillStyle = '#555'; ctx.font = '10px SF Mono, Menlo, Consolas, monospace';
  ctx.textBaseline = 'top';
  ctx.fillText('Log Capacity Used', cx, cy + 2);

  // 0% and 100% labels
  ctx.fillStyle = '#555'; ctx.textAlign = 'left';
  ctx.fillText('0%', cx - radius - 5, cy + 2);
  ctx.textAlign = 'right';
  ctx.fillText('100%', cx + radius + 5, cy + 2);
}

function rate(cur, prev, dt, key) {
  if (!prev || !dt) return 0;
  return Math.max(0, Math.round((cur[key] - (prev[key]||0)) / dt));
}

async function refresh() {
  try {
    const r = await fetch('/api/stats');
    const d = await r.json();
    const s = d.system;
    const my = d.mysql;
    const pl = d.processlist || [];

    // CPU
    let cpuHtml = m('Usage', s.cpu_percent.toFixed(1) + '%') + bar(s.cpu_percent, 'bar-cpu');
    cpuHtml += m('Load (1/5/15)', s.load_avg.map(v => v.toFixed(1)).join(' / '));
    cpuHtml += m('Cores', s.cpu_count);
    document.getElementById('cpu').innerHTML = cpuHtml;

    // Memory
    let memHtml = m('Used / Total', fmtBytes(s.mem_used) + ' / ' + fmtBytes(s.mem_total));
    memHtml += bar(s.mem_percent, 'bar-mem');
    memHtml += m('Available', fmtBytes(s.mem_available));
    memHtml += m('Swap', fmtBytes(s.swap_used) + ' / ' + fmtBytes(s.swap_total));
    document.getElementById('mem').innerHTML = memHtml;

    // Disk
    let diskHtml = m('Used / Total', fmtBytes(s.disk_used) + ' / ' + fmtBytes(s.disk_total));
    diskHtml += bar(s.disk_percent, 'bar-disk');
    diskHtml += m('Free', fmtBytes(s.disk_free));
    document.getElementById('disk').innerHTML = diskHtml;

    // Network
    const now = Date.now() / 1000;
    const dt = prevTime ? (now - prevTime) : 0;
    let netHtml = m('Sent', fmtBytes(s.net_sent));
    netHtml += m('Recv', fmtBytes(s.net_recv));
    if (prevStats && dt > 0) {
      netHtml += m('Send Rate', fmtBytes(Math.round((s.net_sent - prevStats.net_sent)/dt)) + '/s');
      netHtml += m('Recv Rate', fmtBytes(Math.round((s.net_recv - prevStats.net_recv)/dt)) + '/s');
    }
    document.getElementById('net').innerHTML = netHtml;

    const st = document.getElementById('mysql-status');
    if (my.error) {
      st.className = 'status status-err';
      document.getElementById('mysql').innerHTML = `<div class="metric"><span class="label">${my.error}</span></div>`;
      ['innodb-txn','innodb-io','innodb-bp','innodb-rows','cmd-counters','locks','processlist','deadlock'].forEach(
        id => { const el = document.getElementById(id); if(el) el.innerHTML = ''; });
    } else {
      st.className = 'status status-ok';

      // QPS chart
      if (prevStats && prevStats.questions !== undefined && dt > 0) {
        const instantQps = Math.round((my.questions - prevStats.questions) / dt);
        qpsData.push(Math.max(0, instantQps));
        if (qpsData.length > MAX_POINTS) qpsData.shift();
        document.getElementById('qps-current').textContent = fmt(instantQps) + ' QPS';
      }
      drawChart();
      drawHistogram();

      // History list length chart
      hllData.push(my.history_list_length);
      if (hllData.length > MAX_POINTS) hllData.shift();
      const hllVal = my.history_list_length;
      const hllEl = document.getElementById('hll-current');
      hllEl.textContent = fmt(hllVal);
      hllEl.style.color = hllVal >= 100000 ? '#ef4444' : hllVal >= 10000 ? '#fbbf24' : '#a78bfa';
      drawHllChart();

      // Flushing activity chart
      if (prevStats && dt > 0) {
        flushSeries.flushed_ps.push(rate(my, prevStats, dt, 'bp_pages_flushed'));
        flushSeries.log_fsyncs_ps.push(rate(my, prevStats, dt, 'os_log_fsyncs'));
        // Purge TPS: use purge_trx_id delta (MySQL/Percona), or estimate from commit rate - HLL growth (MariaDB)
        let purgeTps = rate(my, prevStats, dt, 'purge_trx_id');
        if (purgeTps === 0 && my.purge_trx_id === 0) {
          // MariaDB fallback: purge rate ≈ commit_rate - history_list_growth_rate
          const commitRate = rate(my, prevStats, dt, 'com_commit') + rate(my, prevStats, dt, 'com_rollback');
          const hllDelta = (my.history_list_length - (prevStats.history_list_length || 0)) / dt;
          purgeTps = Math.max(0, Math.round(commitRate - hllDelta));
        }
        flushSeries.purge_tps.push(purgeTps);
        if (flushSeries.flushed_ps.length > MAX_POINTS) flushSeries.flushed_ps.shift();
        if (flushSeries.log_fsyncs_ps.length > MAX_POINTS) flushSeries.log_fsyncs_ps.shift();
        if (flushSeries.purge_tps.length > MAX_POINTS) flushSeries.purge_tps.shift();
      }
      const dirtyPct = my.bp_total_pages > 0 ? (my.bp_dirty_pages / my.bp_total_pages * 100) : 0;
      flushSeries.dirty_pct.push(dirtyPct);
      if (flushSeries.dirty_pct.length > MAX_POINTS) flushSeries.dirty_pct.shift();
      let logFillPct = 0;
      if (my.redo_log_capacity > 0) {
        logFillPct = my.redo_log_logical_size / my.redo_log_capacity * 100;
      } else if (my.checkpoint_max_age > 0) {
        logFillPct = my.checkpoint_age / my.checkpoint_max_age * 100;
      }
      flushSeries.log_fill_pct.push(Math.min(logFillPct, 100));
      if (flushSeries.log_fill_pct.length > MAX_POINTS) flushSeries.log_fill_pct.shift();
      drawFlushChart();

      // MySQL Dashboard
      let myHtml = '';
      myHtml += m('Uptime', fmtSec(my.uptime));
      myHtml += m('Connections', my.threads_connected + ' / ' + my.max_connections);
      myHtml += m('Running Threads', my.threads_running, my.threads_running > 50 ? 'val-red' : my.threads_running > 20 ? 'val-yellow' : 'val-green');
      myHtml += m('Questions', fmt(my.questions));
      myHtml += m('Avg QPS', fmt(my.qps));
      myHtml += m('Slow Queries', fmt(my.slow_queries), my.slow_queries > 0 ? 'val-yellow' : '');
      myHtml += m('Aborted Clients', my.aborted_clients);
      myHtml += m('Aborted Connects', my.aborted_connects);
      myHtml += m('Bytes Sent', fmtBytes(my.bytes_sent));
      myHtml += m('Bytes Recv', fmtBytes(my.bytes_received));
      myHtml += m('Open Tables', my.open_tables);
      myHtml += m('Threads Created', fmt(my.threads_created));
      document.getElementById('mysql').innerHTML = myHtml;

      // InnoDB Transactions
      let txnHtml = '';
      txnHtml += m('History List Len', fmt(my.history_list_length), my.history_list_length > 100000 ? 'val-red' : my.history_list_length > 10000 ? 'val-yellow' : '');
      txnHtml += m('Active Txns (est)', my.trx_active || '0');
      txnHtml += m('Lock Structs', fmt(my.row_lock_structs));
      txnHtml += m('Lock Time (ms)', fmt(my.row_lock_time));
      txnHtml += m('Avg Lock Wait (ms)', my.row_lock_time_avg);
      txnHtml += m('Max Lock Wait (ms)', fmt(my.row_lock_time_max));
      txnHtml += m('Deadlocks', my.deadlocks, my.deadlocks > 0 ? 'val-red' : '');
      if (my.purge_trx_id > 0) {
        txnHtml += m('Purge TxID', fmt(my.purge_trx_id));
      }
      if (prevStats && dt > 0) {
        let purgeTpsDisplay = rate(my, prevStats, dt, 'purge_trx_id');
        if (purgeTpsDisplay === 0 && my.purge_trx_id === 0) {
          const cr = rate(my, prevStats, dt, 'com_commit') + rate(my, prevStats, dt, 'com_rollback');
          const hd = (my.history_list_length - (prevStats.history_list_length || 0)) / dt;
          purgeTpsDisplay = Math.max(0, Math.round(cr - hd));
        }
        txnHtml += m('Purge TPS (est)', fmt(purgeTpsDisplay), 'val-green');
      }
      txnHtml += m('BP Wait Free', my.bp_wait_free, my.bp_wait_free > 0 ? 'val-red' : '');
      document.getElementById('innodb-txn').innerHTML = txnHtml;

      // InnoDB I/O
      let ioHtml = '';
      ioHtml += m('Data Reads', fmt(my.data_reads));
      ioHtml += m('Data Writes', fmt(my.data_writes));
      if (prevStats && dt > 0) {
        ioHtml += m('Reads/s', fmt(rate(my, prevStats, dt, 'data_reads')), 'val-blue');
        ioHtml += m('Writes/s', fmt(rate(my, prevStats, dt, 'data_writes')), 'val-blue');
        ioHtml += m('Log Writes/s', fmt(rate(my, prevStats, dt, 'log_writes')), 'val-blue');
      }
      ioHtml += m('Pending Reads', my.pending_reads, my.pending_reads > 0 ? 'val-yellow' : '');
      ioHtml += m('Pending Writes', my.pending_writes, my.pending_writes > 0 ? 'val-yellow' : '');
      ioHtml += m('OS Log Fsyncs', fmt(my.os_log_fsyncs));
      document.getElementById('innodb-io').innerHTML = ioHtml;

      // InnoDB Buffer Pool
      const bpPct = my.bp_size > 0 ? (my.bp_data / my.bp_size * 100).toFixed(1) : 0;
      let bpHtml = '';
      bpHtml += m('Size', fmtBytes(my.bp_size));
      bpHtml += m('Data', fmtBytes(my.bp_data) + ' (' + bpPct + '%)');
      bpHtml += bar(parseFloat(bpPct), 'bar-mem');
      bpHtml += m('Dirty Pages', fmtBytes(my.bp_dirty), my.bp_dirty > 1073741824 ? 'val-yellow' : '');
      bpHtml += m('Free Pages', fmt(my.bp_free));
      bpHtml += m('Hit Rate', my.bp_hit_rate + '%', my.bp_hit_rate >= 99 ? 'val-green' : my.bp_hit_rate >= 95 ? 'val-yellow' : 'val-red');
      bpHtml += m('Read Requests', fmt(my.bp_read_requests));
      bpHtml += m('Disk Reads', fmt(my.bp_disk_reads));
      bpHtml += m('Pages Flushed', fmt(my.bp_pages_flushed));
      document.getElementById('innodb-bp').innerHTML = bpHtml;

      // InnoDB Redo Log
      let logPct, logHtml = '';
      if (my.redo_log_capacity > 0) {
        // MySQL 8.x with performance_schema redo log metrics
        logPct = my.redo_log_logical_size / my.redo_log_capacity * 100;
        logHtml += m('Capacity', fmtBytes(my.redo_log_capacity));
        logHtml += m('Logical Size', fmtBytes(my.redo_log_logical_size), logPct >= 90 ? 'val-red' : logPct >= 70 ? 'val-yellow' : '');
        logHtml += m('Physical Size', fmtBytes(my.redo_log_physical_size));
        logHtml += m('Redo Files', my.redo_log_files + ' (' + my.redo_log_files_full + ' full)');
        logHtml += m('LSN Current', fmt(my.lsn_current));
        logHtml += m('LSN Checkpoint', fmt(my.lsn_checkpoint));
        logHtml += m('Unflushed', fmtBytes(my.lsn_current - my.lsn_flushed));
      } else {
        // MariaDB / older MySQL with checkpoint_age
        logPct = my.checkpoint_max_age > 0 ? (my.checkpoint_age / my.checkpoint_max_age * 100) : 0;
        logHtml += m('Log File Size', fmtBytes(my.log_file_size));
        logHtml += m('Checkpoint Age', fmtBytes(my.checkpoint_age), logPct >= 90 ? 'val-red' : logPct >= 70 ? 'val-yellow' : '');
        logHtml += m('Max Checkpoint Age', fmtBytes(my.checkpoint_max_age));
        logHtml += m('LSN Current', fmt(my.lsn_current));
        logHtml += m('LSN Checkpoint', fmt(my.lsn_checkpoint));
        logHtml += m('Unflushed', fmtBytes(my.lsn_current - my.lsn_flushed));
      }
      logHtml += m('Log Waits', my.log_waits, my.log_waits > 0 ? 'val-red' : '');
      logHtml += m('Log Write Requests', fmt(my.log_write_requests));
      if (prevStats && dt > 0) {
        logHtml += m('Log Writes/s', fmt(rate(my, prevStats, dt, 'log_writes')), 'val-blue');
      }
      document.getElementById('innodb-log').innerHTML = logHtml;
      drawLogGauge(Math.min(logPct, 100));

      // InnoDB Row Operations
      let rowHtml = '';
      rowHtml += m('Rows Read', fmt(my.rows_read));
      rowHtml += m('Rows Inserted', fmt(my.rows_inserted));
      rowHtml += m('Rows Updated', fmt(my.rows_updated));
      rowHtml += m('Rows Deleted', fmt(my.rows_deleted));
      if (prevStats && dt > 0) {
        rowHtml += m('Read/s', fmt(rate(my, prevStats, dt, 'rows_read')), 'val-blue');
        rowHtml += m('Insert/s', fmt(rate(my, prevStats, dt, 'rows_inserted')), 'val-blue');
        rowHtml += m('Update/s', fmt(rate(my, prevStats, dt, 'rows_updated')), 'val-blue');
        rowHtml += m('Delete/s', fmt(rate(my, prevStats, dt, 'rows_deleted')), 'val-blue');
      }
      document.getElementById('innodb-rows').innerHTML = rowHtml;

      // Command Counters
      let cmdHtml = '';
      cmdHtml += m('SELECT', fmt(my.com_select));
      cmdHtml += m('INSERT', fmt(my.com_insert));
      cmdHtml += m('UPDATE', fmt(my.com_update));
      cmdHtml += m('DELETE', fmt(my.com_delete));
      cmdHtml += m('REPLACE', fmt(my.com_replace));
      cmdHtml += m('COMMIT', fmt(my.com_commit));
      cmdHtml += m('ROLLBACK', fmt(my.com_rollback));
      if (prevStats && dt > 0) {
        cmdHtml += m('Commit/s', fmt(rate(my, prevStats, dt, 'com_commit')), 'val-blue');
        cmdHtml += m('TPS (Commit+Rollback)', fmt(rate(my, prevStats, dt, 'com_commit') + rate(my, prevStats, dt, 'com_rollback')), 'val-green');
      }
      document.getElementById('cmd-counters').innerHTML = cmdHtml;

      // Locks & Waits
      let lockHtml = '';
      lockHtml += m('Row Lock Waits', fmt(my.row_lock_waits), my.row_lock_waits > 0 ? 'val-yellow' : '');
      lockHtml += m('Row Lock Current', my.row_lock_current, my.row_lock_current > 0 ? 'val-red' : '');
      lockHtml += m('Table Locks Wait', fmt(my.table_locks_waited));
      lockHtml += m('Table Locks Immediate', fmt(my.table_locks_immediate));
      lockHtml += m('Innodb Lock Wait Secs', my.lock_wait_secs || '0');
      if (prevStats && dt > 0) {
        lockHtml += m('Lock Waits/s', fmt(rate(my, prevStats, dt, 'row_lock_waits')), 'val-blue');
      }
      document.getElementById('locks').innerHTML = lockHtml;

      // Processlist
      if (pl.length === 0) {
        document.getElementById('processlist').innerHTML = '<span class="label">No active queries</span>';
      } else {
        let tbl = '<table class="proctable"><tr><th>ID</th><th>User</th><th>Host</th><th>DB</th><th>Cmd</th><th>Time</th><th>State</th><th>Query</th></tr>';
        for (const p of pl) {
          const timeCls = p.time >= 30 ? 'time-crit' : p.time >= 5 ? 'time-warn' : '';
          tbl += `<tr>
            <td>${p.id}</td><td>${escHtml(p.user)}</td><td>${escHtml(p.host)}</td>
            <td>${escHtml(p.db||'')}</td><td>${escHtml(p.command)}</td>
            <td class="${timeCls}">${fmtSec(p.time)}</td>
            <td class="state">${escHtml(p.state||'')}</td>
            <td class="query" title="${escHtml(p.info||'')}">${escHtml((p.info||'').substring(0,120))}</td></tr>`;
        }
        tbl += '</table>';
        document.getElementById('processlist').innerHTML = tbl;
      }

      // Deadlock
      document.getElementById('deadlock').textContent = my.deadlock_info || 'No deadlock detected';
    }

    // Save prev for rate calculation
    prevStats = { ...s, ...my };
    prevTime = now;

    document.getElementById('updated').textContent = 'Updated: ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('updated').textContent = 'Error: ' + e.message;
  }
}

refresh();
setInterval(refresh, 2000);

// Histogram period buttons
document.getElementById('hist-period-btns').addEventListener('click', function(e) {
  const btn = e.target.closest('.period-btn');
  if (!btn) return;
  document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  histPeriodSecs = parseInt(btn.dataset.secs);
  drawHistogram();
});

</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/stats")
def stats():
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    try:
        disk = psutil.disk_usage("/data")
    except FileNotFoundError:
        disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()

    system = {
        "cpu_percent": psutil.cpu_percent(interval=0.3),
        "cpu_count": psutil.cpu_count(),
        "load_avg": list(psutil.getloadavg()),
        "mem_total": mem.total,
        "mem_used": mem.used,
        "mem_available": mem.available,
        "mem_percent": mem.percent,
        "swap_total": swap.total,
        "swap_used": swap.used,
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_free": disk.free,
        "disk_percent": disk.percent,
        "net_sent": net.bytes_sent,
        "net_recv": net.bytes_recv,
    }

    mysql_data = get_mysql_stats()
    processlist = get_processlist()

    return jsonify({"system": system, "mysql": mysql_data, "processlist": processlist})


def _int(status, key, default=0):
    return int(status.get(key, default))


def get_mysql_stats():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG, connection_timeout=3)
        cursor = conn.cursor()

        cursor.execute("SHOW GLOBAL STATUS")
        status = dict(cursor.fetchall())

        cursor.execute("SHOW GLOBAL VARIABLES")
        variables = dict(cursor.fetchall())

        # InnoDB engine status for deadlock info and history list
        deadlock_info = ""
        history_list_length = 0
        trx_active = 0
        lock_wait_secs = 0
        try:
            cursor.execute("SHOW ENGINE INNODB STATUS")
            row = cursor.fetchone()
            if row:
                engine_status = row[2] if len(row) > 2 else ""
                # Extract deadlock section
                if "LATEST DETECTED DEADLOCK" in engine_status:
                    start = engine_status.index("LATEST DETECTED DEADLOCK")
                    # Find next section header
                    rest = engine_status[start:]
                    lines = rest.split("\n")
                    dl_lines = []
                    for i, line in enumerate(lines):
                        if i > 0 and line.startswith("---"):
                            break
                        dl_lines.append(line)
                    deadlock_info = "\n".join(dl_lines[:30])

                # Extract history list length
                for line in engine_status.split("\n"):
                    if "History list length" in line:
                        parts = line.strip().split()
                        for j, p in enumerate(parts):
                            if p == "length":
                                history_list_length = int(parts[j + 1])
                                break

                # Count active transactions
                in_txn_section = False
                for line in engine_status.split("\n"):
                    if "TRANSACTIONS" in line and "---" in line:
                        in_txn_section = True
                    elif in_txn_section and line.startswith("---"):
                        break
                    elif in_txn_section and "ACTIVE" in line:
                        trx_active += 1
                    # Lock wait time
                    if "lock wait timeout" in line.lower():
                        pass
        except Exception:
            pass

        # MySQL 8.x redo log files from performance_schema
        redo_log_files = 0
        redo_log_files_full = 0
        try:
            cursor.execute(
                "SELECT COUNT(*) AS cnt, IFNULL(SUM(IS_FULL),0) AS full_cnt "
                "FROM performance_schema.innodb_redo_log_files"
            )
            row = cursor.fetchone()
            if row:
                redo_log_files = int(row[0])
                redo_log_files_full = int(row[1])
        except Exception:
            pass

        cursor.close()
        conn.close()

        reads = _int(status, "Innodb_buffer_pool_read_requests")
        disk_reads = _int(status, "Innodb_buffer_pool_reads")
        hit_rate = round((reads - disk_reads) / reads * 100, 2) if reads > 0 else 0
        uptime = _int(status, "Uptime", 1)

        return {
            # General
            "uptime": uptime,
            "threads_connected": status.get("Threads_connected", "0"),
            "threads_running": int(status.get("Threads_running", "0")),
            "max_connections": variables.get("max_connections", "0"),
            "questions": _int(status, "Questions"),
            "qps": _int(status, "Questions") // max(uptime, 1),
            "slow_queries": _int(status, "Slow_queries"),
            "aborted_clients": status.get("Aborted_clients", "0"),
            "aborted_connects": status.get("Aborted_connects", "0"),
            "bytes_sent": _int(status, "Bytes_sent"),
            "bytes_received": _int(status, "Bytes_received"),
            "open_tables": status.get("Open_tables", "0"),
            "threads_created": _int(status, "Threads_created"),

            # Command counters
            "com_select": _int(status, "Com_select"),
            "com_insert": _int(status, "Com_insert"),
            "com_update": _int(status, "Com_update"),
            "com_delete": _int(status, "Com_delete"),
            "com_replace": _int(status, "Com_replace"),
            "com_commit": _int(status, "Com_commit"),
            "com_rollback": _int(status, "Com_rollback"),

            # InnoDB buffer pool
            "bp_size": int(variables.get("innodb_buffer_pool_size", 0)),
            "bp_data": _int(status, "Innodb_buffer_pool_bytes_data"),
            "bp_dirty": _int(status, "Innodb_buffer_pool_bytes_dirty"),
            "bp_free": _int(status, "Innodb_buffer_pool_pages_free"),
            "bp_hit_rate": hit_rate,
            "bp_read_requests": _int(status, "Innodb_buffer_pool_read_requests"),
            "bp_disk_reads": disk_reads,
            "bp_pages_flushed": _int(status, "Innodb_buffer_pool_pages_flushed"),
            "bp_total_pages": _int(status, "Innodb_buffer_pool_pages_total"),
            "bp_dirty_pages": _int(status, "Innodb_buffer_pool_pages_dirty"),

            # InnoDB rows
            "rows_read": _int(status, "Innodb_rows_read"),
            "rows_inserted": _int(status, "Innodb_rows_inserted"),
            "rows_updated": _int(status, "Innodb_rows_updated"),
            "rows_deleted": _int(status, "Innodb_rows_deleted"),

            # InnoDB redo log — MySQL 8.x (performance_schema)
            "redo_log_capacity": _int(status, "Innodb_redo_log_capacity_resized"),
            "redo_log_logical_size": _int(status, "Innodb_redo_log_logical_size"),
            "redo_log_physical_size": _int(status, "Innodb_redo_log_physical_size"),
            "redo_log_files": redo_log_files,
            "redo_log_files_full": redo_log_files_full,
            # InnoDB redo log — MariaDB / fallback
            "log_file_size": int(variables.get("innodb_log_file_size", 0)),
            "checkpoint_age": _int(status, "Innodb_checkpoint_age"),
            "checkpoint_max_age": _int(status, "Innodb_checkpoint_max_age"),
            # Common LSN metrics
            "lsn_current": _int(status, "Innodb_redo_log_current_lsn") or _int(status, "Innodb_lsn_current"),
            "lsn_flushed": _int(status, "Innodb_redo_log_flushed_to_disk_lsn") or _int(status, "Innodb_lsn_flushed"),
            "lsn_checkpoint": _int(status, "Innodb_redo_log_checkpoint_lsn") or _int(status, "Innodb_lsn_last_checkpoint"),
            "log_waits": _int(status, "Innodb_log_waits"),
            "log_write_requests": _int(status, "Innodb_log_write_requests"),

            # InnoDB I/O
            "data_reads": _int(status, "Innodb_data_reads"),
            "data_writes": _int(status, "Innodb_data_writes"),
            "log_writes": _int(status, "Innodb_log_writes"),
            "pending_reads": _int(status, "Innodb_data_pending_reads"),
            "pending_writes": _int(status, "Innodb_data_pending_writes"),
            "os_log_fsyncs": _int(status, "Innodb_os_log_fsyncs"),

            # Purge activity
            "purge_trx_id": _int(status, "Innodb_purge_trx_id"),
            "purge_undo_no": _int(status, "Innodb_purge_undo_no"),
            "bp_wait_free": _int(status, "Innodb_buffer_pool_wait_free"),
            "dblwr_pages_written": _int(status, "Innodb_dblwr_pages_written"),
            "data_fsyncs": _int(status, "Innodb_data_fsyncs"),

            # InnoDB transactions & locks
            "history_list_length": history_list_length,
            "trx_active": trx_active,
            "row_lock_structs": _int(status, "Innodb_row_lock_current_waits"),
            "row_lock_waits": _int(status, "Innodb_row_lock_waits"),
            "row_lock_current": _int(status, "Innodb_row_lock_current_waits"),
            "row_lock_time": _int(status, "Innodb_row_lock_time"),
            "row_lock_time_avg": status.get("Innodb_row_lock_time_avg", "0"),
            "row_lock_time_max": _int(status, "Innodb_row_lock_time_max"),
            "deadlocks": _int(status, "Innodb_deadlocks", 0),
            "table_locks_waited": _int(status, "Table_locks_waited"),
            "table_locks_immediate": _int(status, "Table_locks_immediate"),
            "lock_wait_secs": lock_wait_secs,

            # Deadlock
            "deadlock_info": deadlock_info,
        }
    except Exception as e:
        return {"error": str(e)}


def get_processlist():
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG, connection_timeout=3)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT ID as id, USER as user, HOST as host, DB as db, "
            "COMMAND as command, TIME as time, STATE as state, INFO as info "
            "FROM information_schema.PROCESSLIST "
            "WHERE COMMAND != 'Sleep' AND COMMAND != 'Daemon' AND INFO IS NOT NULL "
            "AND ID != CONNECTION_ID() "
            "ORDER BY TIME DESC LIMIT 50"
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception:
        return []



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
