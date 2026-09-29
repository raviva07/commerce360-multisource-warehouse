> Scope: consult [README](../README.md) and [REVIEW](REVIEW.md) for tested local behavior. Cloud/distributed mappings in these learning notes are illustrative and unvalidated.

# Interview Guide
1. **Why SCD2?** To answer questions using attributes as they were at transaction time rather than only today's customer state.
2. **Why checkpoints plus idempotent fact keys?** Checkpoints reduce work; unique business keys protect correctness if a source replays.
3. **How would Airflow improve this?** Scheduling, retries, task visibility, backfills, dependencies and operational metadata.
4. **Why separate Bronze/Silver/Warehouse/Gold?** Replayability, quality boundaries, modeled truth and business-serving outputs.
5. **What happens when the customer API fails?** Retry the isolated ingestion task; downstream tasks do not run until dependencies succeed.
