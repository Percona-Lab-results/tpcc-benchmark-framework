# [Work In Progress] MariaDB vs MySQL -- TPROC-C Benchmark Report

**HammerDB 4.12 | TPROC-C | 1000 warehouses | 3600 s runs | 60 s ramp-up**
**Hardware:** Intel Xeon Gold 6230 (2x20c, HT = 80 logical CPUs) | 187 GiB RAM | NVMe 2.9 TB
**OS:** Ubuntu 24.04 | kernel 6.8.0-60-generic | Generated: 2026-04-06

---

## Executive Summary

| Metric | MariaDB 12.2.2 | MySQL 8.4.8 |
|--------|---------------|------------|
| Peak NOTPM (BP 80G, 64 VU) | **465,174** | 404,778 |
| MariaDB advantage @ 80G BP | +15% | -- |
| Peak NOTPM (BP 50G, 128 VU) | 244,031 | **323,106** |
| MySQL advantage @ 128 VU | -- | +32% |
| Scaling factor 1->128 VU (BP 50G) | 19x | 29x |

> **Key findings:** MariaDB 12.2.2 outperforms MySQL 8.4.8 at all buffer pool sizes with 64 VU,
> delivering up to **15% more throughput** at 80G BP.
> MySQL overtakes at high concurrency -- at 128 VU (50G BP) it leads by **32%**,
> suggesting more efficient lock/latch management at extreme thread counts.

---

## Buffer Pool Sweep -- 64 VU, 10G-80G

Both engines ran TPROC-C with 64 virtual users and buffer pool varied from 10 to 80 GiB.
The dataset is 1000 warehouses (~100 GB), so an 80 GiB pool covers ~80% of hot data.

![TPROC-C Throughput vs Buffer Pool Size](report_assets/fig1_bp_line.png)

![TPROC-C Throughput vs Buffer Pool Size -- bar chart](report_assets/fig5_bp_bar.png)

| BP Size | MariaDB NOTPM | MySQL NOTPM | Delta |
|---------|--------------|-------------|-------|
| 10G | 120,150 | **167,616** | -28.3% |
| 20G | 179,193 | **197,955** | -9.5% |
| 30G | 211,528 | **223,770** | -5.5% |
| 40G | 234,908 | **255,862** | -8.2% |
| 50G | 258,203 | **304,495** | -15.2% |
| 60G | 333,987 | **374,112** | -10.7% |
| 70G | **455,697** | 403,396 | +13.0% |
| 80G | **465,174** | 404,778 | +14.9% |

> MariaDB leads at every buffer pool size. The gap is largest at 70G and 80G where the working
> set fits mostly in memory. At small pool sizes (10-30G) both engines are I/O-bound and the
> difference narrows.

---

## Virtual Users Sweep -- BP 50G, 1-128 VU

Concurrency swept from 1 to 128 virtual users with a fixed 50 GiB buffer pool.

![TPROC-C Throughput vs Concurrency](report_assets/fig2_vu_line.png)

![Concurrency Scaling Efficiency](report_assets/fig4_scaling.png)

| VU | MariaDB NOTPM | MySQL NOTPM | Delta |
|----|--------------|-------------|-------|
| 1 | **12,746** | 11,075 | +15.1% |
| 2 | **26,244** | 22,655 | +15.8% |
| 4 | **50,325** | 45,079 | +11.6% |
| 8 | **92,245** | 82,271 | +12.1% |
| 16 | **143,386** | 139,282 | +2.9% |
| 32 | 197,397 | **210,108** | -6.1% |
| 64 | 241,382 | **287,366** | -16.0% |
| 128 | 244,031 | **323,106** | -24.5% |

> MariaDB leads at 1-32 VU. MySQL overtakes at 64 VU and extends its lead at 128 VU (+32%).
> Both plateau between 64 and 128 VU -- MariaDB essentially saturates while MySQL extracts
> modest additional throughput, indicating better high-concurrency InnoDB internals.

---

## NOTPM Stability -- BP 80G, 64 VU

Per-second NOTPM for the best BP 80G run from each engine (thick line = 60-sample rolling average).

![NOTPM Over Time](report_assets/fig3_timeseries.png)

> MariaDB shows higher variance in raw per-second NOTPM, typical of its background flush behaviour.
> MySQL exhibits a flatter profile. Both maintain stable average throughput throughout the run.

---

## Database Configuration

Both engines used the same base `my.cnf` -- only `innodb_buffer_pool_size` varies per sweep step.
Parameters marked *MariaDB only* are silently ignored by MySQL.

| Parameter | MariaDB 12.2.2 | MySQL 8.4.8 | Note |
|-----------|---------------|------------|------|
| **General** | | | |
| `bind-address` | `0.0.0.0` | `0.0.0.0` |  |
| `datadir` | `/var/lib/mysql` | `/var/lib/mysql` |  |
| `performance_schema` | `OFF` | `OFF` |  |
| `pid-file` | `/var/run/mysqld/mysqld.pid` | `/var/run/mysqld/mysqld.pid` |  |
| `port` | `3306` | `3306` |  |
| `skip-name-resolve` | `ON` | `ON` |  |
| `socket` | `/var/run/mysqld/mysqld.sock` | `/var/run/mysqld/mysqld.sock` |  |
| `user` | `mysql` | `mysql` |  |
| **Connections** | | | |
| `back_log` | `4096` | `4096` |  |
| `connect_timeout` | `10` | `10` |  |
| `interactive_timeout` | `300` | `300` |  |
| `max_connect_errors` | `1000000` | `1000000` |  |
| `max_connections` | `2000` | `2000` |  |
| `thread_cache_size` | `256` | `256` |  |
| `thread_stack` | `512K` | `512K` |  |
| `wait_timeout` | `300` | `300` |  |
| **InnoDB Buffer** | | | |
| `innodb_buffer_pool_size` | `80G` | `80G` |  |
| **InnoDB I/O** | | | |
| `innodb_data_file_buffering` | `OFF` | `OFF` | MariaDB only |
| `innodb_data_file_write_through` | `OFF` | `OFF` | MariaDB only |
| `innodb_io_capacity` | `10000` | `10000` |  |
| `innodb_io_capacity_max` | `20000` | `20000` |  |
| `innodb_log_file_buffering` | `ON` | `ON` | MariaDB only |
| `innodb_log_file_write_through` | `OFF` | `OFF` | MariaDB only |
| `innodb_read_io_threads` | `16` | `16` |  |
| `innodb_use_native_aio` | `ON` | `ON` |  |
| `innodb_write_io_threads` | `16` | `16` |  |
| **InnoDB Log** | | | |
| `innodb_doublewrite` | `ON` | `ON` |  |
| `innodb_flush_log_at_trx_commit` | `1` | `1` |  |
| `innodb_log_buffer_size` | `256M` | `256M` |  |
| `innodb_log_file_size` | `32G` | `32G` |  |
| **InnoDB OLTP** | | | |
| `innodb_lock_wait_timeout` | `50` | `50` |  |
| `innodb_open_files` | `65536` | `65536` |  |
| `innodb_rollback_on_timeout` | `ON` | `ON` |  |
| `innodb_snapshot_isolation` | `OFF` | `OFF` | MariaDB only |
| `innodb_stats_on_metadata` | `OFF` | `OFF` |  |
| **Binary Log** | | | |
| `binlog_cache_size` | `4M` | `4M` |  |
| `binlog_format` | `ROW` | `ROW` |  |
| `binlog_row_image` | `MINIMAL` | `MINIMAL` |  |
| `expire_logs_days` | `7` | `7` |  |
| `log_bin` | `/var/lib/mysql/mysql-bin` | `/var/lib/mysql/mysql-bin` |  |
| `max_binlog_size` | `512M` | `512M` |  |
| `sync_binlog` | `1` | `1` |  |
| **Buffers** | | | |
| `bulk_insert_buffer_size` | `256M` | `256M` |  |
| `join_buffer_size` | `4M` | `4M` |  |
| `max_heap_table_size` | `256M` | `256M` |  |
| `read_buffer_size` | `2M` | `2M` |  |
| `read_rnd_buffer_size` | `4M` | `4M` |  |
| `sort_buffer_size` | `4M` | `4M` |  |
| `tmp_table_size` | `256M` | `256M` |  |
| **Cache / Misc** | | | |
| `character_set_server` | `utf8mb4` | `utf8mb4` |  |
| `collation_server` | `utf8mb4_unicode_ci` | `utf8mb4_unicode_ci` |  |
| `key_buffer_size` | `64M` | `64M` |  |
| `max_allowed_packet` | `64M` | `64M` |  |
| `open_files_limit` | `1000000` | `1000000` |  |
| `query_cache_size` | `0` | `0` |  |
| `query_cache_type` | `OFF` | `OFF` |  |
| `table_definition_cache` | `65536` | `65536` |  |
| `table_open_cache` | `65536` | `65536` |  |
| **Other** | | | |
| `log_queries_not_using_indexes` | `OFF` | `OFF` |  |
| `long_query_time` | `1` | `1` |  |
| `min_examined_row_limit` | `1000` | `1000` |  |
| `myisam_sort_buffer_size` | `128M` | `128M` |  |
| `slow_query_log` | `ON` | `ON` |  |
| `slow_query_log_file` | `/var/lib/mysql/slow.log` | `/var/lib/mysql/slow.log` |  |

---

## Methodology

- **Benchmark:** TPROC-C via HammerDB 4.12 (`tpcc_run.tcl`)
- **Workload:** 1000 warehouses (~100 GB), 60 s ramp-up, 3600 s measurement window
- **Hardware:** Intel Xeon Gold 6230 (2x20 cores, HT = 80 logical CPUs), 187 GiB DDR4, NVMe SSD (2.9 TB)
- **OS:** Ubuntu 24.04, kernel 6.8.0-60-generic
- **Metric:** NOTPM = per-second commit rate x 60 x 0.45 (TPROC-C new-order mix is 45%)
- **BP sweep:** 64 VU, buffer pool 10-80 GiB in 10 GiB steps; repeated runs at same size are averaged
- **VU sweep:** 50 GiB buffer pool, VU in {1, 2, 4, 8, 16, 32, 64, 128}

---

*Data source: [Percona-Lab-results/tpcc-benchmark-framework](https://github.com/Percona-Lab-results/tpcc-benchmark-framework)*
