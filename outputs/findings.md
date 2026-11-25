**Pipeline.** The 2024 batch (8 hourly files, 786 MB gzip) produced 1,870,675 bronze rows and 1,870,675 silver rows (0 quarantined). Re-running the batch left the counts unchanged (idempotent).

**Upstream change.** The same eight hours in 2026 hold 38,471 events against 1,870,675 in 2024 (-97.9%). 14 of 21 contract checks fail: CreateEvent (share 0.00% (min 2.76%)); PushEvent (share 0.00% (min 14.67%)); DeleteEvent (share 0.00% (min 0.72%)); pull_request.merged present (0% of PR events); pull_request.additions present (0% of PR events); no unknown event types (DiscussionEvent). All 8 hourly volume checks fail. Decision: hourly_activity: hold back, repo_activity: hold back, bot_share: publish.

**Optimisations.** Counting by type took 7.4 s on raw JSON and 0.29 s on Parquet (25x). Partition pruning cut a distinct-count from 0.69 s to 0.28 s; the broadcast join ran in 0.53 s against 0.80 s with a shuffle.

**Engines.** On the same gold query over the silver Parquet: Spark (local, 2 cores) 0.45 s, DuckDB 0.05 s, Polars (lazy) 0.04 s. At this size a single-node engine is faster; Spark earns its overhead when data or concurrency outgrow one machine.