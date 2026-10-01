# Stock Market Data Platform — Project Documentation

## Project Overview

The **Stock Market Data Platform** is an end-to-end data engineering project for stock market data. It supports two ingestion patterns: daily market data from Alpha Vantage through a batch pipeline and real-time trade events from Finnhub WebSocket through a streaming pipeline.

Both pipelines land data in Amazon S3 before being processed in Databricks using a Bronze → Silver → Data Quality → Gold architecture. The resulting datasets are used by analytics and observability dashboards.

This document describes the current architecture, ingestion flows, incremental processing strategy, scheduling, data quality, observability, limitations, and future improvements implemented in the project.

---

## Objectives and Scope

- Ingest stock market data using both batch and streaming patterns
- Use AWS as the ingestion and runtime layer
- Use Databricks as the transformation, lakehouse, and analytics layer
- Organize datasets into Bronze, Silver, and Gold layers
- Process new data incrementally using Auto Loader, checkpoints, Delta `MERGE`, and pipeline watermarks
- Validate data quality before Gold processing
- Record audit and freshness information for operational monitoring
- Build analytics-ready datasets for dashboards
- Demonstrate practical, production-oriented data engineering patterns in a portfolio project

---

## Architecture and Data Flow

| Pipeline | Main Flow | Pattern |
|---|---|---|
| Alpha Vantage | EventBridge Scheduler → AWS Lambda → Alpha Vantage API → S3 Landing → Databricks Bronze → Silver → Data Quality → Gold | Batch daily OHLCV data |
| Finnhub | EventBridge Scheduler controls ECS start/stop; data flows Finnhub WebSocket → ECS Producer → Kinesis → ECS Consumer → S3 Raw → Databricks Bronze → Silver → Data Quality → Gold | Real-time trade streaming |

EventBridge Scheduler is used in both ingestion pipelines but serves different purposes. For Alpha Vantage, it triggers AWS Lambda. For Finnhub, it controls the start and stop schedule of the ECS Fargate services around U.S. market hours.

See [architecture.md](architecture.md) for the architecture diagram.

---

## Batch Pipeline: Alpha Vantage

### Ingestion Flow

EventBridge Scheduler triggers AWS Lambda on the configured schedule.

The Lambda function:

1. Reads the Alpha Vantage API key from AWS Secrets Manager
2. Calls the Alpha Vantage `TIME_SERIES_DAILY` endpoint
3. Receives the JSON response
4. Writes the payload to the Amazon S3 Landing Zone
5. Supports multiple stock symbols

Primary S3 bucket:

```text
s3://stock-market-data-platform-pait/
```

Alpha Vantage landing structure:

```text
landing/alphavantage/daily/
    symbol=AAPL/
        ingestion_date=YYYY-MM-DD/
            <timestamp>.json
```

### AWS Schedule

EventBridge Scheduler:

```text
06:00 Asia/Bangkok
Tuesday - Saturday
```

This schedule runs after the previous U.S. trading session so that the daily market data can be collected before Databricks processing begins.

---

## Streaming Pipeline: Finnhub

### Control Flow

EventBridge Scheduler controls the ECS Fargate producer and consumer services according to U.S. market hours.

```text
09:25 America/New_York
Start Consumer

09:28 America/New_York
Start Producer

16:02 America/New_York
Stop Producer

16:05 America/New_York
Stop Consumer
```

Starting the consumer before the producer ensures that the consumer is ready before new trade events are published.

### Data Flow

```text
Finnhub WebSocket
→ ECS Fargate Producer
→ Amazon Kinesis Data Streams
→ ECS Fargate Consumer
→ Amazon S3 Raw
```

### Producer

File:

```text
src/streaming/finnhub_producer.py
```

The producer:

- Reads the Finnhub API key from AWS Secrets Manager
- Connects to Finnhub WebSocket
- Subscribes to trade events
- Normalizes incoming trade records
- Publishes records to Amazon Kinesis Data Streams

Core trade fields:

```text
symbol
price
timestamp
volume
source
```

Kinesis stream:

```text
stock-trades-stream
```

### Consumer

File:

```text
src/streaming/kinesis_consumer.py
```

The consumer:

- Reads Kinesis shards
- Buffers trade records into batches
- Writes JSONL files to Amazon S3

Current settings:

```text
BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 30
ShardIteratorType = LATEST
```

S3 raw path:

```text
s3://stock-market-data-platform-pait/raw/trades/
```

Partition structure:

```text
raw/trades/
    year=YYYY/
        month=MM/
            day=DD/
                hour=HH/
                    trades-<uuid>.jsonl
```

The current consumer uses `LATEST`, so records created before the consumer begins reading are not automatically replayed.

### ECS

Cluster:

```text
stock-streaming-cluster
```

Services:

```text
stock-streaming-producer-service
stock-streaming-consumer-service
```

Task definitions:

```text
stock-streaming-producer
stock-streaming-consumer
```

When the streaming pipeline is not active, the services are kept at `desiredCount = 0` to reduce AWS cost.

---

## Databricks Medallion Architecture

Databricks uses the Unity Catalog catalog:

```text
workspace
```

Main schemas:

```text
workspace.bronze
workspace.silver
workspace.gold
```

### Bronze Layer

The Bronze layer stores raw or minimally transformed data together with source metadata.

Responsibilities include:

- Incremental ingestion from Amazon S3
- Basic schema casting
- Capturing source file metadata
- Preserving source data as closely as possible to its original form

### Silver Layer

The Silver layer is responsible for:

- Symbol normalization
- Data type casting
- Timestamp conversion
- Trade-date derivation
- Deduplication
- Business-key enforcement

### Data Quality Layer

Data quality validation runs after Silver and before Gold:

```text
Bronze
↓
Silver
↓
Data Quality
↓
Gold
```

If the DQ task fails, the Gold task does not run.

### Gold Layer

The Gold layer contains analytics-ready datasets used by dashboards.

Finnhub Gold tables:

```text
workspace.gold.daily_trade_summary
workspace.gold.hourly_trade_summary
```

Alpha Vantage Gold table:

```text
workspace.gold.alphavantage_daily_metrics
```

---

## Finnhub Medallion Pipeline

### Bronze

Table:

```text
workspace.bronze.finnhub_trades
```

Auto Loader checkpoint:

```text
/Volumes/workspace/bronze/checkpoints/finnhub_trades
```

Schema location:

```text
/Volumes/workspace/bronze/checkpoints/finnhub_trades_schema
```

Auto Loader uses `availableNow=True` to process new files that have not been ingested previously.

Bronze casts the main fields to:

```text
price       DOUBLE
source      STRING
symbol      STRING
timestamp   INT
volume      INT
year        INT
month       INT
day         INT
hour        INT
```

It also stores metadata fields such as:

```text
_ingested_at
_source_file
_source_file_name
_source_file_modified_at
```

### Silver

Table:

```text
workspace.silver.finnhub_trades
```

Business key:

```text
symbol + price + timestamp + volume + source
```

Silver reads Bronze rows newer than the latest Silver ingestion timestamp and writes new records using Delta `MERGE`.

The current merge behavior inserts only records that do not already exist according to the business key.

### Gold

Finnhub Gold uses the watermark:

```text
finnhub_gold
```

Gold identifies Silver rows newer than the stored watermark and derives affected keys.

Daily aggregation key:

```text
symbol + trade_date
```

Hourly aggregation key:

```text
symbol + trade_date + trade_hour
```

Only affected groups are recomputed from the full Silver dataset and then merged back into Gold.

The watermark is updated only after both daily and hourly merges complete successfully.

---

## Alpha Vantage Medallion Pipeline

### Bronze

Table:

```text
workspace.bronze.alphavantage_daily
```

Checkpoint:

```text
/Volumes/workspace/bronze/checkpoints/alphavantage_daily
```

Schema location:

```text
/Volumes/workspace/bronze/checkpoints/alphavantage_daily_schema
```

Alpha Vantage Bronze stores the source payload as strings:

```text
data_json
metadata_json
```

It also stores:

```text
_ingested_at
_source_file
_source_file_name
_source_file_modified_at
```

### Silver

Table:

```text
workspace.silver.alphavantage_daily
```

Business key:

```text
symbol + trade_date
```

Silver parses `Time Series (Daily)` into a map, explodes the daily entries, casts OHLCV values to typed columns, and deduplicates records.

Main columns:

```text
symbol
trade_date
open
high
low
close
volume
source_ingestion_timestamp
_source_file
```

Alpha Vantage Silver uses the watermark:

```text
alphavantage_silver
```

For matched records, the merge updates the target only when the incoming `source_ingestion_timestamp` is newer than the existing record.

### Gold

Table:

```text
workspace.gold.alphavantage_daily_metrics
```

Gold metrics include:

```text
previous_close
daily_return_pct
price_range
price_range_pct
previous_volume
volume_change_pct
ma_5
ma_20
```

Gold watermark:

```text
alphavantage_gold
```

Because metrics such as lag values and moving averages require historical context, the pipeline recomputes the full history for affected symbols and then merges the recomputed results back into Gold.

---

## Incremental Processing Strategy

The platform avoids unnecessary full refreshes by combining several incremental-processing techniques.

### Auto Loader

Bronze ingestion uses Databricks Auto Loader to detect new files in Amazon S3.

Benefits include:

- Avoiding repeated full file scans
- Tracking previously processed files through checkpoints
- Supporting incremental ingestion

### Checkpoints

Managed Volume:

```text
workspace.bronze.checkpoints
```

The volume stores Auto Loader state for the ingestion pipelines.

### Delta MERGE

Silver and Gold use Delta `MERGE` instead of overwriting entire tables.

General pattern:

```text
new records
→ identify affected keys
→ recompute required data
→ MERGE
```

### Pipeline Watermarks

Table:

```text
workspace.gold.pipeline_watermarks
```

Columns:

```text
pipeline_name
last_processed_ingested_at
```

Current pipeline identifiers:

```text
finnhub_gold
alphavantage_silver
alphavantage_gold
```

Watermarks are updated only after the associated processing step succeeds.

---

## Data Quality

Data quality validation is positioned between Silver and Gold.

### Finnhub DQ

Checks include:

- Null or empty symbol
- Null or non-positive price
- Null or non-positive volume
- Duplicate groups based on the business key

Business key:

```text
symbol + price + timestamp + volume + source
```

### Alpha Vantage DQ

Checks include:

- Null or empty symbol
- Null trade date
- Null or non-positive open/high/low/close values
- Null or non-positive volume
- `high < low`
- Duplicate `symbol + trade_date` groups

### Failure Behavior

DQ status is recorded as:

```text
PASSED
FAILED
```

The audit row is written before the notebook raises an exception.

This means failed DQ runs are still preserved in the audit history.

If DQ fails:

```text
Silver
↓
DQ FAILED
↓
Gold does not run
```

---

## Observability and Audit

Audit table:

```text
workspace.gold.pipeline_audit
```

Main fields:

```text
run_time
pipeline_name
layer
status
row_count
latest_trade_date
watermark
invalid_symbol
invalid_price
invalid_volume
duplicate_groups
invalid_trade_date
invalid_high_low
```

The audit table acts as an append-only historical log.

### Observability Dashboard

The current observability dashboard contains:

- Latest Pipeline Status
- Latest Row Count
- DQ Error Summary
- Freshness Status
- Freshness Days
- Audit Run History

Freshness is currently calculated by comparing `latest_trade_date` with the current date.

A known limitation is that the current freshness logic is calendar-day based and does not account for U.S. market holidays.

---

## Scheduling and Orchestration

The platform uses both AWS EventBridge Scheduler and Databricks Jobs.

### Alpha Vantage AWS Schedule

```text
06:00 Asia/Bangkok
Tuesday - Saturday
```

Flow:

```text
EventBridge Scheduler
→ Lambda
→ Alpha Vantage API
→ S3 Landing
```

### Alpha Vantage Databricks Job

Job:

```text
stock-market-alphavantage-medallion
```

Schedule:

```text
06:20 Asia/Bangkok
Tuesday - Saturday
```

Task dependency:

```text
bronze_alphavantage
↓
silver_alphavantage
↓
dq_alphavantage
↓
gold_alphavantage
```

### Finnhub AWS Schedule

```text
09:25 America/New_York  Start Consumer
09:28 America/New_York  Start Producer
16:02 America/New_York  Stop Producer
16:05 America/New_York  Stop Consumer
```

Runs Monday through Friday.

### Finnhub Databricks Job

Job:

```text
stock-market-finnhub-medallion
```

Schedule:

```text
16:25 America/New_York
Monday - Friday
```

Task dependency:

```text
bronze_finnhub
↓
silver_finnhub
↓
dq_finnhub
↓
gold_finnhub
```

---

## Analytics Dashboard

Dashboard:

```text
Stock Market Trading Activity Dashboard
```

### Finnhub Metrics

- Total Volume
- Total Trades
- Hourly Trading Volume
- Hourly Trade Count
- Hourly VWAP

### Alpha Vantage Metrics

- Symbol Filter
- Close Price Trend
- MA5 vs MA20
- Daily Return %
- Daily Volume
- Daily High / Low

Databricks dashboards may require a refresh before newly processed data becomes visible, even when the underlying Delta tables are already up to date.

---

## Repository Structure

Approximate repository structure:

```text
stock-market-data-platform/
│
├── README.md
├── .gitignore
├── Dockerfile
├── requirements.txt
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── project_documentation.md
│
└── src/
    └── streaming/
        ├── finnhub_producer.py
        └── kinesis_consumer.py
```

Sensitive credentials such as API keys, AWS credentials, and Databricks tokens must not be committed to the repository.

---

## Security and Credential Management

Finnhub secret:

```text
stock-market-data-platform/finnhub
```

Application code uses AWS IAM roles or the default AWS credential chain instead of hardcoded access keys.

Databricks uses read-only external access to S3 through:

```text
Storage Credential:
stock_market_s3_read_credential

External Location:
stock_market_s3_raw
```

S3 URL:

```text
s3://stock-market-data-platform-pait/
```

This approach allows Databricks to read data from Amazon S3 without storing AWS access keys inside notebooks.

---

## Cost Management

The project was designed with AWS cost control in mind.

Current cost-management practices include:

- Keeping ECS services at `desiredCount = 0` when not in use
- Deleting the Kinesis stream when it is not needed for testing and recreating it before the streaming window
- Using scheduled start/stop behavior instead of running streaming compute continuously
- Using Databricks Free Edition for transformation and analytics

An AWS Budget Alert is also used to monitor cloud spending.

---

## Known Limitations

### Market Calendar

Freshness status currently uses calendar-day differences.

Weekends and U.S. market holidays can therefore cause the dashboard to display WARNING or STALE even when the pipeline is operating correctly.

### Kinesis Replay

The consumer currently uses:

```text
LATEST
```

As a result, events created before the consumer begins reading are not replayed automatically.

### Streaming Durability

The current consumer implementation focuses on the batch-to-S3 flow and does not yet include an advanced replay or offset-recovery strategy.

### Dashboard Refresh

Databricks dashboards may still show older data until they are refreshed.

### Kinesis Cost Optimization

The Kinesis stream may be deleted when it is not being used in order to reduce cost. It must therefore be recreated and verified as `Active` before the streaming ingestion window begins.

---

## Future Improvements

Potential improvements include:

- Alerting when DQ checks fail
- Alerting when freshness becomes STALE
- Market-calendar-aware freshness monitoring
- CI/CD deployment
- Infrastructure as Code, such as Terraform
- Automated integration testing
- Improved Kinesis replay and checkpoint strategy
- Additional stock symbols
- Additional market data sources
- Centralized operational logging and metrics
- Pipeline reliability metrics once enough audit history is available

---

## Summary

The Stock Market Data Platform covers multiple core data engineering capabilities, including batch ingestion, streaming ingestion, an S3-based data lake, Databricks Medallion Architecture, incremental processing, Delta `MERGE`, pipeline watermarks, data quality validation, observability, and analytics dashboards.

The architecture separates ingestion runtime from transformation responsibilities. AWS handles ingestion and streaming runtime, while Databricks handles data transformation, quality validation, lakehouse processing, and analytics-ready datasets.

For table schemas and column definitions, see [data_dictionary.md](data_dictionary.md). For the architecture diagram, see [architecture.md](architecture.md).
