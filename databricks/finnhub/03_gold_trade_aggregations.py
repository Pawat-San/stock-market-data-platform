# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "workspace.silver.finnhub_trades"
DAILY_GOLD_TABLE = "workspace.gold.daily_trade_summary"
HOURLY_GOLD_TABLE = "workspace.gold.hourly_trade_summary"

WATERMARK_TABLE = "workspace.gold.pipeline_watermarks"

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DAILY_GOLD_TABLE} (
        symbol STRING,
        trade_date DATE,
        low_price DOUBLE,
        high_price DOUBLE,
        total_volume BIGINT,
        trade_count BIGINT,
        avg_trade_price DOUBLE,
        vwap DOUBLE,
        first_trade_timestamp TIMESTAMP,
        last_trade_timestamp TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HOURLY_GOLD_TABLE} (
        symbol STRING,
        trade_date DATE,
        trade_hour INT,
        low_price DOUBLE,
        high_price DOUBLE,
        total_volume BIGINT,
        trade_count BIGINT,
        avg_trade_price DOUBLE,
        vwap DOUBLE,
        first_trade_timestamp TIMESTAMP,
        last_trade_timestamp TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        pipeline_name STRING,
        last_processed_ingested_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

existing = (
    spark.table(WATERMARK_TABLE)
    .filter(F.col("pipeline_name") == "finnhub_gold")
    .count()
)

if existing == 0:
    # NULL means no Silver rows have been published to Gold yet.
    spark.sql("""
        INSERT INTO workspace.gold.pipeline_watermarks
        VALUES ('finnhub_gold', CAST(NULL AS TIMESTAMP))
    """)
    print("Initial Finnhub Gold watermark created.")
elif existing > 1:
    raise RuntimeError("Multiple finnhub_gold watermark rows found; repair the table before running Gold.")
else:
    print("Finnhub Gold watermark already exists.")

# COMMAND ----------

watermark = (
    spark.table(WATERMARK_TABLE)
    .filter(F.col("pipeline_name") == "finnhub_gold")
    .select("last_processed_ingested_at")
    .collect()[0][0]
)

print("Current watermark:", watermark)

# Older runs may have advanced the watermark before Gold was populated.
# Rebuild both summaries together if either table is empty.
gold_needs_backfill = (
    spark.table(DAILY_GOLD_TABLE).limit(1).count() == 0
    or spark.table(HOURLY_GOLD_TABLE).limit(1).count() == 0
)
if gold_needs_backfill:
    print("An empty Gold summary requires a full Silver backfill.")

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)

df_silver_incremental = (
    df_silver
    # Recompute boundary ties; daily/hourly MERGEs are idempotent.
    .filter(
        F.col("_ingested_at") >= F.lit(watermark)
        if watermark is not None and not gold_needs_backfill else F.lit(True)
    )
)

print(
    "New Silver rows for gold",
    df_silver_incremental.count()
)


# COMMAND ----------

affected_daily = (
    df_silver_incremental
    .select("symbol", "trade_date")
    .distinct()
)

display(affected_daily)

# COMMAND ----------

affected_hourly = (
    df_silver_incremental
    .withColumn(
        "trade_hour",
        F.hour("trade_timestamp")
    )
    .select("symbol", "trade_date", "trade_hour")
    .distinct()
)

display(affected_hourly)


# COMMAND ----------

if df_silver_incremental.limit(1).count() > 0:

    df_daily_recomputed = (
        df_silver
        .join(
            affected_daily,
            on=["symbol", "trade_date"],
            how="inner"
        )
        .groupBy("symbol", "trade_date")
        .agg(
            F.min("price").alias("low_price"),
            F.max("price").alias("high_price"),
            F.sum("volume").alias("total_volume"),
            F.count("*").alias("trade_count"),
            F.avg("price").alias("avg_trade_price"),
            (
                F.sum(F.col("price") * F.col("volume"))
                / F.sum("volume")
            ).alias("vwap"),
            F.min("trade_timestamp").alias("first_trade_timestamp"),
            F.max("trade_timestamp").alias("last_trade_timestamp")
        )
    )

    daily_delta = DeltaTable.forName(
        spark,
        DAILY_GOLD_TABLE
    )

    (
        daily_delta.alias("target")
        .merge(
            df_daily_recomputed.alias("source"),
            """
            target.symbol = source.symbol
            AND target.trade_date = source.trade_date
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Daily Gold MERGE completed.")

else:
    print("No new Silver rows. Daily Gold unchanged.")

# COMMAND ----------

if df_silver_incremental.limit(1).count() > 0:

    df_hourly_source = (
        df_silver
        .withColumn(
            "trade_hour",
            F.hour("trade_timestamp")
        )
    )

    df_hourly_recomputed = (
        df_hourly_source
        .join(
            affected_hourly,
            on=["symbol", "trade_date", "trade_hour"],
            how="inner"
        )
        .groupBy(
            "symbol",
            "trade_date",
            "trade_hour"
        )
        .agg(
            F.min("price").alias("low_price"),
            F.max("price").alias("high_price"),
            F.sum("volume").alias("total_volume"),
            F.count("*").alias("trade_count"),
            F.avg("price").alias("avg_trade_price"),
            (
                F.sum(F.col("price") * F.col("volume"))
                / F.sum("volume")
            ).alias("vwap"),
            F.min("trade_timestamp").alias("first_trade_timestamp"),
            F.max("trade_timestamp").alias("last_trade_timestamp")
        )
    )

    hourly_delta = DeltaTable.forName(
        spark,
        HOURLY_GOLD_TABLE
    )

    (
        hourly_delta.alias("target")
        .merge(
            df_hourly_recomputed.alias("source"),
            """
            target.symbol = source.symbol
            AND target.trade_date = source.trade_date
            AND target.trade_hour = source.trade_hour
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Hourly Gold MERGE completed.")

else:
    print("No new Silver rows. Hourly Gold unchanged.")

# COMMAND ----------

if df_silver_incremental.limit(1).count() > 0:

    new_watermark = (
        df_silver_incremental
        .agg(F.max("_ingested_at").alias("max_ingested_at"))
        .collect()[0]["max_ingested_at"]
    )

    watermark_delta = DeltaTable.forName(
        spark,
        WATERMARK_TABLE
    )

    (
        watermark_delta.alias("target")
        .merge(
            spark.createDataFrame(
                [("finnhub_gold", new_watermark)],
                ["pipeline_name", "last_processed_ingested_at"]
            ).alias("source"),
            "target.pipeline_name = source.pipeline_name"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Gold watermark updated to:", new_watermark)

else:
    print("No new Silver rows. Watermark unchanged.")
