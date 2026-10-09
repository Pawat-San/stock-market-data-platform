# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType
)

df_quality = spark.table("workspace.silver.finnhub_trades")

invalid_symbol = (
    df_quality
    .filter(
        F.col("symbol").isNull() |
        (F.trim(F.col("symbol")) == "")
    )
    .count()
)

invalid_price = (
    df_quality
    .filter(
        F.col("price").isNull() |
        (F.col("price") <= 0) |
        F.isnan("price") |
        (F.col("price") == float("inf"))
    )
    .count()
)

invalid_volume = (
    df_quality
    .filter(
        F.col("volume").isNull() |
        (F.col("volume") <= 0)
    )
    .count()
)

invalid_source = df_quality.filter(
    F.col("source").isNull() | (F.trim("source") == "")
).count()

duplicate_groups = (
    df_quality
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

invalid_timestamp = df_quality.filter(
    F.col("timestamp").isNull() |
    (F.col("timestamp") <= 0) |
    (F.col("timestamp") > 253402300799999) |
    F.col("trade_timestamp").isNull() |
    F.col("trade_date").isNull()
).count()
invalid_trade_date = df_quality.filter(F.col("trade_date").isNull()).count()

print("invalid_symbol:", invalid_symbol)
print("invalid_price:", invalid_price)
print("invalid_volume:", invalid_volume)
print("invalid_source:", invalid_source)
print("duplicate_groups:", duplicate_groups)
print("invalid_timestamp:", invalid_timestamp)

# COMMAND ----------

quality_errors = []

if invalid_symbol > 0:
    quality_errors.append(f"invalid_symbol={invalid_symbol}")

if invalid_price > 0:
    quality_errors.append(f"invalid_price={invalid_price}")

if invalid_volume > 0:
    quality_errors.append(f"invalid_volume={invalid_volume}")

if invalid_source > 0:
    quality_errors.append(f"invalid_source={invalid_source}")

if duplicate_groups > 0:
    quality_errors.append(f"duplicate_groups={duplicate_groups}")

if invalid_timestamp > 0:
    quality_errors.append(f"invalid_timestamp={invalid_timestamp}")

dq_status = "FAILED" if quality_errors else "PASSED"

# COMMAND ----------

AUDIT_TABLE = "workspace.gold.pipeline_audit"
SILVER_TABLE = "workspace.silver.finnhub_trades"
WATERMARK_TABLE = "workspace.gold.pipeline_watermarks"

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.gold.pipeline_audit (
        run_time TIMESTAMP,
        pipeline_name STRING,
        layer STRING,
        status STRING,
        row_count BIGINT,
        latest_trade_date DATE,
        watermark TIMESTAMP,
        invalid_symbol BIGINT,
        invalid_price BIGINT,
        invalid_volume BIGINT,
        duplicate_groups BIGINT,
        invalid_trade_date INT,
        invalid_high_low INT
    ) USING DELTA
""")

df_silver = spark.table(SILVER_TABLE)

row_count = df_silver.count()

latest_trade_date = (
    df_silver
    .agg(F.max("trade_date").alias("latest_trade_date"))
    .collect()[0]["latest_trade_date"]
)


watermark_rows = (
    spark.table(WATERMARK_TABLE)
    .filter(F.col("pipeline_name") == "finnhub_gold")
    .select("last_processed_ingested_at")
    .limit(1)
    .collect()
) if spark.catalog.tableExists(WATERMARK_TABLE) else []
watermark = watermark_rows[0][0] if watermark_rows else None

# COMMAND ----------

audit_schema = StructType([
    StructField("pipeline_name", StringType()),
    StructField("layer", StringType()),
    StructField("status", StringType()),
    StructField("row_count", LongType()),
    StructField("latest_trade_date", DateType()),
    StructField("watermark", TimestampType()),
    StructField("invalid_symbol", LongType()),
    StructField("invalid_price", LongType()),
    StructField("invalid_volume", LongType()),
    StructField("duplicate_groups", LongType()),
])

audit_df = spark.createDataFrame(
    [(
        "finnhub",
        "silver",
        dq_status,
        row_count,
        latest_trade_date,
        watermark,
        invalid_symbol,
        invalid_price,
        invalid_volume,
        duplicate_groups
    )],
    audit_schema
).withColumn(
    "run_time",
    F.current_timestamp()
).withColumn(
    "invalid_trade_date",
    F.lit(invalid_trade_date).cast("int")
).withColumn(
    "invalid_high_low",
    F.lit(None).cast("int")
).select(
    "run_time",
    "pipeline_name",
    "layer",
    "status",
    "row_count",
    "latest_trade_date",
    "watermark",
    "invalid_symbol",
    "invalid_price",
    "invalid_volume",
    "duplicate_groups",
    "invalid_trade_date",
    "invalid_high_low"
)

# COMMAND ----------

audit_df.write.mode("append").saveAsTable(AUDIT_TABLE)

print("Finnhub audit row written.")

# COMMAND ----------

if quality_errors:
    raise Exception(
        "Finnhub Silver data quality failed: "
        + ", ".join(quality_errors)
    )

print("Finnhub Silver data quality passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_time,
# MAGIC     pipeline_name,
# MAGIC     status,
# MAGIC     row_count,
# MAGIC     invalid_symbol,
# MAGIC     invalid_price,
# MAGIC     invalid_volume,
# MAGIC     duplicate_groups,
# MAGIC     invalid_trade_date,
# MAGIC     invalid_high_low
# MAGIC FROM workspace.gold.pipeline_audit
# MAGIC ORDER BY run_time DESC;
