# Finnhub Databricks validation cases

Run these cases in an isolated Databricks test workspace or with temporary
tables and paths. Keep the production job stopped while exercising a test
table. The local `unittest` suite covers producer IDs and JSONL transport;
these cases cover Delta and job behavior.

| Case | Setup and action | Expected result |
|---|---|---|
| Multi-line JSONL | Place a JSONL file containing two valid trade objects in the test raw path. Run Bronze. | Bronze contains two rows with distinct `event_id` values and source file metadata. |
| Malformed numeric value | Place a trade with `"price":"bad"`. Run Bronze, then Silver. | Bronze retains `_raw_price = 'bad'` and `price IS NULL`; Silver excludes the row. If an older saved schema rejects the field type, inspect `_rescued_data` and the original JSONL file. |
| Same market values, different IDs | Place two trades with identical symbol, price, timestamp, volume, and source but different `event_id`. Run Bronze → Silver → Gold. | Silver has two rows; Gold `trade_count = 2` and `total_volume` includes both. |
| UTC boundary | Use trades at `2026-01-01T23:59:59.999Z` and `2026-01-02T00:00:00.000Z`. Run Silver → Gold. | `trade_date` is January 1 and 2 respectively; Gold hourly keys use hours 23 and 0. |
| Rejected row checkpoint | Append only an invalid trade to Bronze and run Silver twice. | `finnhub.silver.last_bronze_version` advances after the first run; the second run has no CDF inserts to process. |
| Silver rerun | Run Silver twice against the same Bronze version. | Silver row count and `event_id` set are unchanged. |
| Gold rerun | Run Gold twice with no new Silver rows. | Daily/hourly values and `finnhub_gold` watermark are unchanged. |
| Silver changes during Gold | Start Gold, then commit a new Silver batch before both Gold merges finish. | Daily, hourly, and watermark from that Gold run reflect the same pinned Silver version; the next Gold run picks up the later batch. |
| Failure after daily merge | In a test schema, make the hourly target incompatible so Gold fails after the daily merge. Repair the hourly target and rerun Gold. | Daily merge is idempotent; hourly summary completes; watermark advances only on the successful run. |
| Concurrent Gold runs | Launch two test Gold runs at the same time. | One acquires `finnhub_gold_run_lock`; the other fails or is queued by the Job's `max_concurrent_runs = 1`. |

The Gold lock is released in `finally` after a normal notebook exception. If a
cluster terminates without running cleanup, wait for the 24-hour lease to
expire or clear `owner_id` and `expires_at` only after confirming no Gold run is
active. Change Data Feed history must remain available back to the stored
Bronze version; if retention removes it, perform a controlled full backfill
and reset `finnhub.silver.last_bronze_version` after verifying the target.
