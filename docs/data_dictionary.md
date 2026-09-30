# Data Dictionary

## Finnhub

### workspace.bronze.finnhub_trades

Purpose:
Stores raw streaming trade records ingested from Amazon S3.

| Column | Type | Description |
|---|---|---|
| symbol | STRING | Stock ticker symbol |
| price | DOUBLE | Trade price |
| timestamp | INT | Trade timestamp in milliseconds |
| volume | INT | Trade volume |
| source | STRING | Data source |
| year | INT | Partition year |
| month | INT | Partition month |
| day | INT | Partition day |
| hour | INT | Partition hour |
| _ingested_at | TIMESTAMP | Databricks ingestion timestamp |
| _source_file | STRING | Source file path |