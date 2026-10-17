# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_TABLE = "workspace.bronze.finnhub_trades"
SILVER_TABLE = "workspace.silver.finnhub_trades"
SILVER_SCHEMA_VERSION = "event_id_utc_v1"
BRONZE_VERSION_PROPERTY = "finnhub.silver.last_bronze_version"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Enable CDF before the initial snapshot. Later runs read Bronze commits only.
bronze_properties = (
    spark.sql(f"DESCRIBE DETAIL {BRONZE_TABLE}")
    .select("properties").collect()[0][0] or {}
)
if bronze_properties.get("delta.enableChangeDataFeed", "false").lower() != "true":
    spark.sql(f"""
        ALTER TABLE {BRONZE_TABLE}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

bronze_version = (
    spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE} LIMIT 1")
    .select("version").collect()[0][0]
)
bronze_snapshot = (
    spark.read.format("delta")
    .option("versionAsOf", bronze_version)
    .table(BRONZE_TABLE)
)
if "event_id" not in bronze_snapshot.columns:
    bronze_snapshot = bronze_snapshot.withColumn("event_id", F.lit(None).cast("string"))

if not spark.catalog.tableExists(SILVER_TABLE):
    (bronze_snapshot.limit(0)
     .withColumn("trade_timestamp", F.expr("timestamp_millis(timestamp)"))
     .withColumn("trade_date", F.to_date("trade_timestamp"))
     .write.format("delta").saveAsTable(SILVER_TABLE))
elif "event_id" not in spark.table(SILVER_TABLE).columns:
    spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD COLUMNS (event_id STRING)")

silver_properties = (
    spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}")
    .select("properties").collect()[0][0] or {}
)
if silver_properties.get("finnhub.silver.schema_version") != SILVER_SCHEMA_VERSION:
    # Old raw events with identical fields cannot be separated retroactively.
    spark.sql(f"""
        UPDATE {SILVER_TABLE}
        SET event_id = concat('legacy:', sha2(to_json(named_struct(
            'symbol', symbol, 'price', price, 'timestamp', timestamp,
            'volume', volume, 'source', source
        )), 256))
        WHERE event_id IS NULL
    """)
    spark.sql(f"""
        UPDATE {SILVER_TABLE}
        SET trade_date = to_date(timestamp_millis(timestamp))
        WHERE NOT (trade_date <=> to_date(timestamp_millis(timestamp)))
    """)
    spark.sql(f"""
        ALTER TABLE {SILVER_TABLE}
        SET TBLPROPERTIES ('finnhub.silver.schema_version' = '{SILVER_SCHEMA_VERSION}')
    """)

last_version_text = silver_properties.get(BRONZE_VERSION_PROPERTY)
last_version = int(last_version_text) if last_version_text is not None else None
if last_version is not None and last_version > bronze_version:
    raise RuntimeError("Silver checkpoint is ahead of Bronze; inspect table history.")

# First run reads a pinned full snapshot. Subsequent runs consume CDF inserts.
# Rejected rows do not recur merely because their _ingested_at ties a valid row.
if last_version is None:
    df_incremental = bronze_snapshot
elif last_version < bronze_version:
    df_incremental = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_version + 1)
        .option("endingVersion", bronze_version)
        .table(BRONZE_TABLE)
        .filter(F.col("_change_type") == "insert")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    if "event_id" not in df_incremental.columns:
        df_incremental = df_incremental.withColumn("event_id", F.lit(None).cast("string"))
else:
    df_incremental = bronze_snapshot.limit(0)

# COMMAND ----------

dedup_window = (
    Window.partitionBy("event_id")
    .orderBy(F.col("_ingested_at").asc(), F.col("_source_file").asc())
)

df_silver_incremental = (
    df_incremental
    .withColumn("symbol", F.upper(F.trim("symbol")))
    .filter(
        F.col("symbol").isNotNull() &
        (F.trim("symbol") != "") &
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
        "event_id",
        F.when(
            F.col("event_id").isNotNull() & (F.trim("event_id") != ""),
            F.concat(F.lit("producer:"), F.trim("event_id"))
        ).otherwise(F.concat(
            F.lit("legacy:"),
            F.sha2(F.to_json(F.struct(
                "symbol", "price", "timestamp", "volume", "source"
            )), 256)
        ))
    )
    .withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .withColumn("trade_timestamp", F.expr("timestamp_millis(timestamp)"))
    .withColumn("trade_date", F.to_date("trade_timestamp"))
)

(
    DeltaTable.forName(spark, SILVER_TABLE).alias("target")
    .merge(df_silver_incremental.alias("source"), "target.event_id = source.event_id")
    .whenNotMatchedInsertAll()
    .execute()
)

# A failed MERGE leaves the checkpoint unchanged. Replay is idempotent.
spark.sql(f"""
    ALTER TABLE {SILVER_TABLE}
    SET TBLPROPERTIES ('{BRONZE_VERSION_PROPERTY}' = '{bronze_version}')
""")
print(f"Finnhub Silver processed Bronze through version {bronze_version}.")
