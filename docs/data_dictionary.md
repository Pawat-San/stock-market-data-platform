# Data Dictionary

This document describes the main Delta tables used in the Stock Market Data Platform.

The tables are organized by data source and processing layer:

- Bronze: raw or minimally transformed ingestion layer
- Silver: cleaned and standardized data
- Gold: analytics-ready aggregates and metrics
- Observability: pipeline audit and watermark metadata

## Finnhub

### workspace.bronze.finnhub_trades

Purpose:
Stores raw streaming trade records ingested from Amazon S3.

| Column | Type | Description |
|---|---|---|
| event_id | STRING | Producer-generated UUID for each received trade; null for historical events |
| symbol | STRING | Stock ticker symbol |
| price | DOUBLE | Trade price |
| timestamp | BIGINT | Trade timestamp in milliseconds |
| volume | BIGINT | Trade volume |
| source | STRING | Data source |
| _raw_price | STRING | Price field before numeric conversion |
| _raw_timestamp | STRING | Timestamp field before numeric conversion |
| _raw_volume | STRING | Volume field before numeric conversion |
| _rescued_data | STRING | Auto Loader JSON for fields that do not match the inferred schema |
| year | INT | Partition year |
| month | INT | Partition month |
| day | INT | Partition day |
| hour | INT | Partition hour |
| _ingested_at | TIMESTAMP | Databricks ingestion timestamp |
| _source_file | STRING | Source file path |

Malformed numeric values remain visible in the `_raw_*` columns even when
`try_cast` returns null. The original JSONL file remains in S3.


### workspace.silver.finnhub_trades

**Purpose**

Stores cleaned, standardized, and deduplicated Finnhub trade records.

| Column | Type | Description |
|---|---|---|
| event_id | STRING | `producer:<UUID>` for new events or `legacy:<hash>` for historical rows |
| symbol | STRING | Normalized stock ticker symbol |
| price | DOUBLE | Trade price |
| timestamp | BIGINT | Original Unix timestamp in milliseconds |
| trade_timestamp | TIMESTAMP | Converted trade timestamp |
| trade_date | DATE | UTC date derived from trade timestamp |
| volume | BIGINT | Trade volume |
| source | STRING | Source identifier |
| _ingested_at | TIMESTAMP | Bronze ingestion timestamp |
| _source_file | STRING | Source file path |

**Business Key**

`event_id`. Historical records without a producer ID retain the previous
`symbol + price + timestamp + volume + source` identity as a deterministic
`legacy:<hash>`; identical historical events cannot be separated retroactively.

Silver stores the last successfully processed Bronze Delta version as table
property `finnhub.silver.last_bronze_version`. Later runs consume Bronze Change
Data Feed inserts after that version.

### workspace.gold.daily_trade_summary

**Purpose**

Stores daily aggregated Finnhub trading metrics by symbol.

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker symbol |
| trade_date | DATE | UTC trading date |
| low_price | DOUBLE | Lowest trade price of the day |
| high_price | DOUBLE | Highest trade price of the day |
| total_volume | BIGINT | Total traded volume |
| trade_count | BIGINT | Number of trade records |
| avg_trade_price | DOUBLE | Average trade price |
| vwap | DOUBLE | Volume Weighted Average Price |
| first_trade_timestamp | TIMESTAMP | First trade timestamp of the day |
| last_trade_timestamp | TIMESTAMP | Last trade timestamp of the day |

### workspace.gold.hourly_trade_summary

**Purpose**

Stores hourly aggregated Finnhub trading metrics by symbol.

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker symbol |
| trade_date | DATE | UTC trading date |
| trade_hour | INT | UTC hour of day (0–23) |
| low_price | DOUBLE | Lowest trade price during the hour |
| high_price | DOUBLE | Highest trade price during the hour |
| total_volume | BIGINT | Total traded volume |
| trade_count | BIGINT | Number of trade records |
| avg_trade_price | DOUBLE | Average trade price |
| vwap | DOUBLE | Volume Weighted Average Price |

## Alpha Vantage

### workspace.bronze.alphavantage_daily

**Purpose**

Stores raw Alpha Vantage daily market snapshots ingested from Amazon S3.

| Column | Type | Description |
|---|---|---|
| data_json | STRING | Raw Alpha Vantage time-series payload stored as JSON text |
| metadata_json | STRING | Raw Alpha Vantage metadata stored as JSON text |
| _ingested_at | TIMESTAMP | Databricks ingestion timestamp |
| _source_file | STRING | Full source file path |
| _source_file_name | STRING | Source file name |
| _source_file_modified_at | TIMESTAMP | Source file modification timestamp |

### workspace.silver.alphavantage_daily

**Purpose**

Stores parsed, cleaned, typed, and deduplicated daily OHLCV records.

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker symbol |
| trade_date | DATE | Trading date |
| open | DOUBLE | Opening price |
| high | DOUBLE | Highest price of the day |
| low | DOUBLE | Lowest price of the day |
| close | DOUBLE | Closing price |
| volume | INT | Daily trading volume |
| source_ingestion_timestamp | TIMESTAMP | Timestamp of the source snapshot used for the record |
| _source_file | STRING | Source file path |

**Business Key**

`symbol + trade_date`

### workspace.gold.alphavantage_daily_metrics

**Purpose**

Stores analytics-ready daily market metrics derived from Alpha Vantage Silver data.

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker symbol |
| trade_date | DATE | Trading date |
| open | DOUBLE | Opening price |
| high | DOUBLE | Daily high price |
| low | DOUBLE | Daily low price |
| close | DOUBLE | Closing price |
| volume | INT | Daily trading volume |
| previous_close | DOUBLE | Previous trading day's closing price |
| daily_return_pct | DOUBLE | Daily percentage return relative to previous close |
| price_range | DOUBLE | Difference between daily high and low |
| price_range_pct | DOUBLE | Daily price range expressed as a percentage |
| previous_volume | INT | Previous trading day's volume |
| volume_change_pct | DOUBLE | Percentage change in volume from previous trading day |
| ma_5 | DOUBLE | 5-day moving average of closing price |
| ma_20 | DOUBLE | 20-day moving average of closing price |


## Pipeline Metadata and Observability

### workspace.gold.pipeline_watermarks

**Purpose**

Stores the latest successfully processed timestamp for incremental pipelines.

| Column | Type | Description |
|---|---|---|
| pipeline_name | STRING | Unique pipeline watermark identifier |
| last_processed_ingested_at | TIMESTAMP | Latest successfully processed ingestion timestamp |

Current pipeline identifiers include:

- `finnhub_gold`
- `alphavantage_silver`
- `alphavantage_gold`

### workspace.gold.finnhub_gold_run_lock

**Purpose**

Contains one row used to prevent overlapping Finnhub Gold notebook runs. The
owner is released when the notebook finishes; a lease expires after 24 hours
if the run terminates without cleanup.

| Column | Type | Description |
|---|---|---|
| pipeline_name | STRING | `finnhub_gold` |
| owner_id | STRING | UUID of the current notebook run, or null |
| expires_at | TIMESTAMP | Lease expiration in UTC, or null |

### workspace.gold.pipeline_audit

**Purpose**

Stores historical pipeline execution, freshness, and data quality metrics.

| Column | Type | Description |
|---|---|---|
| run_time | TIMESTAMP | Timestamp when the audit record was written |
| pipeline_name | STRING | Pipeline name, such as `finnhub` or `alphavantage` |
| layer | STRING | Data layer being validated |
| status | STRING | Data quality result, for example `PASSED` or `FAILED` |
| row_count | INT | Number of records in the validated dataset |
| latest_trade_date | DATE | Most recent trading date available |
| watermark | TIMESTAMP | Pipeline watermark at audit time |
| invalid_symbol | INT | Number of records with invalid symbols |
| invalid_price | INT | Number of records with invalid price values |
| invalid_volume | INT | Number of records with invalid volume values |
| duplicate_groups | INT | Number of duplicate business-key groups |
| invalid_trade_date | INT | Number of invalid trade dates; primarily used by Alpha Vantage |
| invalid_high_low | INT | Number of rows where high/low validation fails; primarily used by Alpha Vantage |
