> Scope: consult [README](../README.md) and [REVIEW](REVIEW.md) for tested local behavior. Cloud/distributed mappings in these learning notes are illustrative and unvalidated.

# Architecture & Design Decisions
- **REST ingestion** is isolated from transformation so API retries do not duplicate warehouse writes.
- **Checkpoints** store the maximum source `updated_at` and make reruns incremental.
- **SCD2** preserves customer location/segment history instead of overwriting it.
- **Fact rows** use source `order_id` as an idempotent business key.
- **Gold marts** are regenerated from trusted warehouse state so they are reproducible.
- **Lineage JSON** makes source→layer→table relationships inspectable.

## Production evolution
Local filesystem → S3/ADLS/GCS; SQLite → PostgreSQL/Redshift/Snowflake; local DAG runner → Airflow 3; local logs → centralized observability.
