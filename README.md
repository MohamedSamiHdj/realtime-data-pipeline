# Realtime Data Pipeline (Windows + PySpark)

A compact, production‑style **batch data pipeline** built for Windows developers. It ingests raw data, writes **bronze** Parquet, runs **data‑quality** checks, and produces a curated **silver** dataset. The orchestration wrapper (`run_full_pipeline.ps1`) applies sensible Windows Spark hygiene so runs are predictable and reproducible.

---

## Table of Contents
- [Architecture](#architecture)
- [Repository Layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
- [Running the Pipeline](#running-the-pipeline)
- [Job Details](#job-details)
- [Outputs](#outputs)
- [Operational Notes](#operational-notes)
- [Performance & Parallelism](#performance--parallelism)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
raw data  ──►  batch_etl.py       ──►  bronze/ (immutable landing zone, Parquet)
           └─► dq_checks.py       ──►  artifacts/ (DQ summary JSONs)
bronze/   ──►  write_silver.py    ──►  silver/ (cleaned, partitioned Parquet)
```

- **Raw**: Original source files kept intact (e.g., `data/raw/yellow_tripdata_2023-01.parquet`).
- **Bronze**: Standardized landing zone; schema-checked, append/overwrite friendly.
- **Silver**: Cleaned and lightly conformed; adds metadata (`silver_loaded_at`) and partitions for downstream analytics.
- **Artifacts**: Time‑stamped DQ results for auditability.

---

## Repository Layout

```
spark_jobs/
  batch_etl.py            # raw → bronze
  dq_checks.py            # validations; writes artifacts/dq_summary_*.json
  write_silver.py         # bronze → silver
  run_full_pipeline.ps1   # Windows wrapper: orchestration + env hygiene

scripts/
  fetch_data.py           # optional: stage sample raw inputs
  simulate_stream.py      # optional: demo generator (not required for batch)

dags/
  etl_daily.py            # example DAG stub

tests/
  test_batch_etl.py       # sample unit test scaffolding

data/
  raw/                    # keep your input files here (not deleted by cleaner)
  bronze/                 # output (created by pipeline)
  silver/                 # output (created by pipeline)

artifacts/                # DQ JSONs (created by pipeline)
logs/                     # step logs (created by pipeline)
```

---

## Prerequisites

- **OS:** Windows 10/11
- **Python:** **3.11.x** (recommended)
- **Java:** JDK 11 (e.g., Adoptium Temurin 11)
- **Dependencies:** installed via `requirements.txt` (uses pip’s PySpark; no external Spark required)

> If you use a standalone Spark, ensure **versions match** (e.g., Spark 4.0.0 ↔ PySpark 4.0.0) or prefer the pip‑installed PySpark to avoid path mismatches on Windows.

---

## Quickstart

```powershell
# 1) Clone
git clone https://github.com/chhuang216/realtime-data-pipeline
cd realtime-data-pipeline

# 2) Create venv (Python 3.11) and install deps
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt

# 3) Place raw data (Such as NYC Yellow Taxi Data)
#   Example parquet: data\raw\yellow_tripdata_2023-01.parquet
```

---

## Running the Pipeline

Run **all steps** (ETL → DQ → Silver) with logging and artifacts:

```powershell
.\spark_jobs\run_full_pipeline.ps1 -WarnOnlyDQ -WriteSilver
```

- **`-WarnOnlyDQ`** — logs DQ violations as warnings (pipeline continues).
- **`-WriteSilver`** — executes silver refinement after ETL and DQ.

To run individual steps during development:

```powershell
# ETL only (raw → bronze)
python .\spark_jobs\batch_etl.py --target-partitions 16 --shuffle-partitions 16

# DQ only
python .\spark_jobs\dq_checks.py

# Silver only (bronze → silver)
python .\spark_jobs\write_silver.py --mode overwrite
```

---

## Job Details

### `batch_etl.py`
- Reads raw Parquet, normalizes column names, derives `pickup_date`, and **writes bronze** partitioned by `pickup_date`.
- **Parallelism controls (defaults chosen for laptops):**
  - `--target-partitions` *(default 16)* — number of output tasks/files.
  - `--shuffle-partitions` *(default 16)* — reduces dev‑time shuffle overhead.
  - `--max-partition-bytes` *(default 64m)* — encourages more input splits from single large files.
  - `--max-records-per-file` *(default 0 = disabled)* — optional cap on output rows per file.

### `dq_checks.py`
- Validates bronze for basic expectations (non‑empty, essential columns present, simple null checks).
- Emits `artifacts/dq_summary_<ts>.json`. Returns non‑zero on hard failures (or warnings if pipeline launched with `-WarnOnlyDQ`).

### `write_silver.py`
- Trims string columns, drops obvious temp columns (e.g., prefixed `_`), adds `silver_loaded_at` timestamp.
- Writes **silver** Parquet (partitioned) for downstream analytics.

---

## Outputs

- **Bronze:** `data/bronze/`
- **Silver:** `data/silver/`
- **DQ summaries:** `artifacts/dq_summary_*.json`
- **Logs:** `logs/step_*.log`

Quick inspection of silver output:
```powershell
python - << 'PY'
import glob, pyarrow.parquet as pq
files = glob.glob("data/silver/**/*.parquet", recursive=True)
print("Found", len(files), "silver file(s)")
if files:
    t = pq.read_table(files[0]); print(t.schema); print("Rows in first file:", t.num_rows)
PY
```

---

## Operational Notes

- The wrapper sets `SPARK_LOCAL_DIRS=C:\tmp\spark` and ensures temp folders exist (reduces Windows‑specific noise).
- If available, place `winutils.exe` under `C:\hadoop\bin\winutils.exe` and ensure it’s on `PATH` for full Hadoop API coverage (not required for local Parquet reads/writes).
- The cleaner (`clean.ps1`) removes logs/artifacts/outputs but **keeps `data/raw/` and its content** intact.

---

## Performance & Parallelism

- Parquet is **splittable**; a single large file still reads in parallel when row groups exist.
- Increase read parallelism by lowering `spark.sql.files.maxPartitionBytes` (the script default is `64m`).
- Control downstream tasks and output file counts with `--target-partitions` or `repartition("pickup_date")`.
- For small dev datasets, set `--shuffle-partitions 16` (Spark default ≈ 200 is overkill locally).

---

## Troubleshooting

**“The system cannot find the path specified.”**  
Usually a path mismatch from external Spark/Hadoop on Windows. Prefer pip PySpark, or ensure `SPARK_HOME`, `HADOOP_HOME`, and temp dirs are set correctly.

**Missing / broken virtualenv (no `pyvenv.cfg`)**  
Recreate the venv:
```powershell
deactivate 2>$null
Remove-Item -Recurse -Force .\.venv
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

*This repository is intentionally small but follows the same engineering principles used in production: layered storage (raw/bronze/silver), explicit parallelism controls, step‑level observability, and Windows‑friendly orchestration.*
