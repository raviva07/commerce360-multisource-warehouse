# Commerce360

**Incremental multi-source warehousing** · Intermediate

Combine orders, product metadata, and customer history into a warehouse with slowly changing customer dimensions.

## What runs in this repository

- CSV orders, JSON products, and a real local HTTP customer endpoint
- Data contracts, timeouts, retryable task dependencies
- Timestamp checkpoints and order upserts
- SCD Type 2 customer versions and event-time dimension lookup
- Revenue marts, lineage, and task metrics

## Data flow

CSV + JSON + local HTTP → Bronze → incremental Silver → SQLite SCD2 warehouse → Gold reports

## Quick start

Use Python 3.12. From this repository's root:

```bash
python -m venv .venv
# Linux / macOS
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python run_local.py
python -m pytest -q
```

No API keys or cloud credentials are required for the local demo. Run the pipeline again to inspect its repeat-run behavior. Tests use temporary project copies and do not erase your working outputs.

## Expected first run

6 order facts · total revenue 254,000 · two historical versions for customer C002. A second unchanged run processes zero order/customer increments.

Generated databases, models, logs, and checkpoints are intentionally excluded from Git. Build them with the quick-start command; a fresh clone must not inherit an old checkpoint or pre-trained binary.

## Configuration and operation

The runner starts and stops its own HTTP server on a free loopback port. Edit `data_contract.json` to inspect the required columns and allowed channels. Inputs live in `data/input/`.

## Repository guide

| Path | Purpose |
| --- | --- |
| `src/` | Runnable local implementation |
| `tests/` | Isolated functional and regression tests |
| `data/` | Example inputs; generated layers are ignored |
| `sql/` | Queries to inspect the warehouse or feature store |
| `docs/` | Architecture, setup, operations, and review notes |
| `.github/workflows/tests.yml` | Python test workflow on pushes and pull requests |

## Scope and trade-offs

Watermarks require strictly increasing source update timestamps; late records or records added with an already-processed timestamp need an explicit backfill. Customer changes must be chronological. Orders earlier than the first known customer version use the earliest version as a documented fallback. Checkpoints and SQLite are separate commits. The DAG runner is sequential and in-process; the Airflow file is an illustrative scaffold.

## Portfolio presentation

Demonstrate a clean run, inspect the SQL tables and output files, then rerun and explain the state handling. Show a failure/regression test and describe the trade-offs above. Describe this as a local portfolio implementation; do not claim distributed throughput, production SLAs, or cloud deployment from the reference files.

See [review and validation](docs/REVIEW.md) for the fixes and test scope. The local implementation and this README define the tested behavior; production mappings in older learning notes are future work.
