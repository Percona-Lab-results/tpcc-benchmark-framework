# Project Instructions for Claude

## Report Workflow

When making any report change, always do all of these in sequence without being asked:
1. Edit `build_gdoc.py` (the source of truth)
2. Regenerate (`python build_gdoc.py`)
3. Update the Google Doc in-place via `gws drive files update --params '{"fileId":"1DBdtv2EPVyCU-FGPwrdU0ifhXh_vNXEQ3B8OHFJGaGA"}' --upload report_gdoc_v2.html --upload-content-type "text/html"`
4. Update git: `git add`, `git commit`, `git push origin main`

Never delete and re-create the Google Doc — always update in-place to preserve the URL.

## Report Architecture

- Source of truth: `build_gdoc.py` on the `main` branch
- Running it produces: `report_gdoc_v2.html` (Google Doc source), `REPORT.md` (markdown with full data)
- Google Doc ID: `1DBdtv2EPVyCU-FGPwrdU0ifhXh_vNXEQ3B8OHFJGaGA`
- The `seekdb_report` branch has a separate SeekDB-specific report
- Data lives in `data/runs.json`; chart PNGs go to `report_assets/`

## Google Doc vs REPORT.md

The Google Doc is the presentation-quality output — keep it concise with charts and commentary but minimal data tables. Data tables (BP iterations, VU iterations, jitter stats) belong in REPORT.md for reference. When adding new sections, default to charts + commentary in Google Doc, full data tables in REPORT.md only.

## Terminology

Use "iterations" instead of "sweep" in all user-facing report text (headings, chart titles, descriptions, methodology). Internal code that filters by directory names containing "sweep" must keep the original string since it matches actual folder names on disk.

## Chart Design

- Add reference lines for hardware context (e.g., vertical lines for physical cores / HT threads on scaling charts)
- Use normalized views (% of mean) when comparing jitter/variance across engines with different absolute values
- Always include a paragraph of commentary explaining what the chart shows and the key takeaways
- Timeseries charts: thin lines for raw 1-sec data, thick lines for rolling average

## Database Configuration Section

- Only include parameters from the actual `.cnf` files in the repo root
- Exclude boilerplate sections: General, Connections, Buffers, Cache/Misc
- Keep only performance-relevant sections: InnoDB Buffer, InnoDB I/O, InnoDB Log, InnoDB OLTP, Binary Log
- Add explanatory commentary for non-obvious config choices
- Key facts:
  - `innodb_buffer_pool_instances` applies to MySQL only — MariaDB uses a single instance
  - 32G redo log is deliberately oversized to avoid log bottleneck
  - `innodb_io_capacity = 10000` to utilise NVMe
  - Direct I/O bypasses OS page cache (do NOT attribute this to `innodb_use_native_aio`)
  - `sync_binlog = 1` and `innodb_flush_log_at_trx_commit = 1` for full durability
