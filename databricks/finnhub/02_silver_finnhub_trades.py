# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_TABLE = "workspace.bronze.finnhub_trades"
SILVER_TABLE = "workspace.silver.finnhub_trades"

df_bronze = spark.table(BRONZE_TABLE)

# The first run must be able to create Silver from existing Bronze data.
if not spark.catalog.tableExists(SILVER_TABLE):
    (df_bronze.limit(0)
     .withColumn("trade_timestamp", F.expr("timestamp_millis(timestamp)"))
     .withColumn("trade_date", F.to_date("trade_timestamp"))
     .write.format("delta").saveAsTable(SILVER_TABLE))

latest_silver_ingested_at = (
    spark
    .table(SILVER_TABLE)
    .select(F.max("_ingested_at").alias("max_ingested_at"))
    .collect()[0]["max_ingested_at"]
)

print("Latest Silver _ingested_at:", latest_silver_ingested_at)

# COMMAND ----------

if latest_silver_ingested_at is None:
    df_incremental = df_bronze
else:
    df_incremental = (
        df_bronze
        # Replay ties at the boundary; MERGE makes this idempotent.
        .filter(F.col("_ingested_at") >= latest_silver_ingested_at)
    )

print("Incremental rows:", df_incremental.count())

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy(
        "symbol",
        "price",
        "timestamp",
        "volume",
        "source"
    )
    .orderBy(
        F.col("_ingested_at").asc(),
        F.col("_source_file").asc()
    )
)

df_silver_incremental = (
    df_incremental
    .withColumn("symbol", F.upper(F.trim("symbol")))
    .filter(
        (F.col("symbol").isNotNull()) &
        (F.trim(F.col("symbol")) != "") &
        F.col("source").isNotNull() &
        (F.trim("source") != "") &
        (F.col("price") > 0) &
        (~F.isnan("price")) &
        (F.col("price") < float("inf")) &
        (F.col("volume") > 0) &
        (F.col("timestamp") > 0) &
        (F.col("timestamp") <= 253402300799999) &
        F.col("_ingested_at").isNotNull()
    )
    .withColumn(
        "_row_number",
        F.row_number().over(dedup_window)
    )
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn(
        "trade_timestamp",
        F.expr("timestamp_millis(timestamp)")
    )
    .withColumn(
        "trade_date",
        F.to_date("trade_timestamp")
    )
)

affected_daily = (
    df_silver_incremental
    .select("symbol", "trade_date")
    .distinct()
)

display(affected_daily)

affected_hourly = (
    df_silver_incremental
    .withColumn(
        "trade_hour",
        F.hour("trade_timestamp")
    )
    .select(
        "symbol",
        "trade_date",
        "trade_hour"
    )
    .distinct()
)

display(affected_hourly)

# COMMAND ----------

silver_delta = DeltaTable.forName(
    spark,
    "workspace.silver.finnhub_trades"
)

(
    silver_delta.alias("target")
    .merge(
        df_silver_incremental.alias("source"),
        """
        target.symbol = source.symbol
        AND target.price = source.price
        AND target.timestamp = source.timestamp
        AND target.volume = source.volume
        AND target.source = source.source
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver MERGE completed.")

# COMMAND ----------

silver_count = spark.table (
    "workspace.silver.finnhub_trades"
).count()

print(f"Silver rows after Merge: {silver_count:,}")

# COMMAND ----------

duplicate_count = (
    spark.table("workspace.silver.finnhub_trades")
    .groupBy(
        "symbol",
        "price",
        "timestamp",
        "volume",
        "source",
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate rows in Silver: {duplicate_count:,}")
