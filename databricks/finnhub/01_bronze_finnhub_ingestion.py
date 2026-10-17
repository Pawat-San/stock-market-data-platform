# Databricks notebook source
from pyspark.sql import functions as F

RAW_PATH = "s3://stock-market-data-platform-pait/raw/trades/"
BRONZE_TABLE = "workspace.bronze.finnhub_trades"

CHECKPOINT_PATH = "/Volumes/workspace/bronze/checkpoints/finnhub_trades"
SCHEMA_PATH = "/Volumes/workspace/bronze/checkpoints/finnhub_trades_schema"

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # On a fresh checkpoint, load files already present in the landing path.
    .option("cloudFiles.includeExistingFiles", "true")
    .load(RAW_PATH)
    .selectExpr(
        "*",
        "_metadata.file_path as _source_file",
        "_metadata.file_name as _source_file_name",
        "_metadata.file_modification_time as _source_file_modified_at"
    )
)

# Historical files do not have event_id. Auto Loader adds the field when it
# discovers newer files; keep the first run with the old schema valid too.
if "event_id" not in df_stream.columns:
    df_stream = df_stream.withColumn("event_id", F.lit(None).cast("string"))
if "_rescued_data" not in df_stream.columns:
    df_stream = df_stream.withColumn("_rescued_data", F.lit(None).cast("string"))

# COMMAND ----------

df_bronze_incremental = (
    df_stream
    .select(
        F.col("event_id").cast("string").alias("event_id"),
        F.expr("try_cast(`price` AS DOUBLE)").alias("price"),
        F.col("source").cast("string").alias("source"),
        F.col("symbol").cast("string").alias("symbol"),
        F.expr("try_cast(`timestamp` AS BIGINT)").alias("timestamp"),
        F.expr("try_cast(`volume` AS BIGINT)").alias("volume"),

        # Keep source field values when a numeric cast produces null. The
        # original JSONL remains in S3 for byte-for-byte inspection.
        F.col("price").cast("string").alias("_raw_price"),
        F.col("timestamp").cast("string").alias("_raw_timestamp"),
        F.col("volume").cast("string").alias("_raw_volume"),
        F.col("_rescued_data"),

        F.current_timestamp().alias("_ingested_at"),

        F.col("_source_file"),
        F.col("_source_file_name"),
        F.col("_source_file_modified_at"),

        F.col("year").cast("int").alias("year"),
        F.col("month").cast("int").alias("month"),
        F.col("day").cast("int").alias("day"),
        F.col("hour").cast("int").alias("hour")
    )
)

# COMMAND ----------

df_bronze_incremental.printSchema()

# COMMAND ----------

query = (
    df_bronze_incremental.writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

print("Finnhub Bronze incremental load completed.")
