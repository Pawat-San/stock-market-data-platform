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

# COMMAND ----------

df_bronze_incremental = (
    df_stream
    .select(
        F.col("price").cast("double").alias("price"),
        F.col("source").cast("string").alias("source"),
        F.col("symbol").cast("string").alias("symbol"),
        F.col("timestamp").cast("long").alias("timestamp"),
        F.col("volume").cast("long").alias("volume"),

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
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()

print("Finnhub Bronze incremental load completed.")
