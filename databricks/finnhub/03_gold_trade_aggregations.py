# Databricks notebook source
import uuid

from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "workspace.silver.finnhub_trades"
DAILY_GOLD_TABLE = "workspace.gold.daily_trade_summary"
HOURLY_GOLD_TABLE = "workspace.gold.hourly_trade_summary"

WATERMARK_TABLE = "workspace.gold.pipeline_watermarks"
GOLD_SCHEMA_VERSION = "event_id_utc_v1"

spark.conf.set("spark.sql.session.timeZone", "UTC")

silver_properties = (
    spark.sql(f"DESCRIBE DETAIL {SILVER_TABLE}")
    .select("properties").collect()[0][0] or {}
)
if silver_properties.get("finnhub.silver.schema_version") != GOLD_SCHEMA_VERSION:
    raise RuntimeError("Run the updated Finnhub Silver notebook before Gold.")
if silver_properties.get("finnhub.silver.last_bronze_version") is None:
    raise RuntimeError("Silver has not completed its first Bronze version checkpoint.")

LOCK_TABLE = "workspace.gold.finnhub_gold_run_lock"
RUN_ID = str(uuid.uuid4())


def acquire_lock():
    # One Delta row is the lock. Conflicting UPDATEs on that row cannot both commit.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOCK_TABLE} USING DELTA AS
        SELECT 'finnhub_gold' AS pipeline_name,
               CAST(NULL AS STRING) AS owner_id,
               CAST(NULL AS TIMESTAMP) AS expires_at
    """)
    rows = spark.table(LOCK_TABLE).filter("pipeline_name = 'finnhub_gold'").collect()
    if len(rows) != 1:
        raise RuntimeError("Finnhub Gold lock table must contain exactly one row.")
    spark.sql(f"""
        UPDATE {LOCK_TABLE}
        SET owner_id = '{RUN_ID}',
            expires_at = current_timestamp() + INTERVAL 24 HOURS
        WHERE pipeline_name = 'finnhub_gold'
          AND (owner_id IS NULL OR expires_at < current_timestamp())
    """)
    owner = (
        spark.table(LOCK_TABLE)
        .filter("pipeline_name = 'finnhub_gold'")
        .select("owner_id").collect()[0][0]
    )
    if owner != RUN_ID:
        raise RuntimeError("Another Finnhub Gold run holds the lock.")


def release_lock():
    spark.sql(f"""
        UPDATE {LOCK_TABLE}
        SET owner_id = NULL, expires_at = NULL
        WHERE pipeline_name = 'finnhub_gold' AND owner_id = '{RUN_ID}'
    """)


def run_gold():
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


    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            pipeline_name STRING,
            last_processed_ingested_at TIMESTAMP
        ) USING DELTA
    """)


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


    watermark = (
        spark.table(WATERMARK_TABLE)
        .filter(F.col("pipeline_name") == "finnhub_gold")
        .select("last_processed_ingested_at")
        .collect()[0][0]
    )

    print("Current watermark:", watermark)

    def gold_version(table_name):
        properties = spark.sql(f"DESCRIBE DETAIL {table_name}").select("properties").collect()[0][0] or {}
        return properties.get("finnhub.gold.schema_version")


    # Rebuild once after the event-ID and UTC migration. Delete keys that belonged
    # to the previous timezone so old daily/hourly groups do not remain in Gold.
    full_rebuild = (
        gold_version(DAILY_GOLD_TABLE) != GOLD_SCHEMA_VERSION
        or gold_version(HOURLY_GOLD_TABLE) != GOLD_SCHEMA_VERSION
    )

    # Older runs may have advanced the watermark before Gold was populated.
    # Rebuild both summaries together if either table is empty.
    gold_needs_backfill = (
        full_rebuild
        or
        spark.table(DAILY_GOLD_TABLE).limit(1).count() == 0
        or spark.table(HOURLY_GOLD_TABLE).limit(1).count() == 0
    )
    if gold_needs_backfill:
        print("Full Silver backfill required for Gold.")


    # Use one Delta snapshot for the watermark and both aggregations, even if
    # another Silver run commits while this Gold run is still active.
    silver_version = (
        spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 1")
        .select("version").collect()[0][0]
    )
    df_silver = (
        spark.read.format("delta")
        .option("versionAsOf", silver_version)
        .table(SILVER_TABLE)
    )

    df_silver_incremental = (
        df_silver
        # Recompute boundary ties; daily/hourly MERGEs are idempotent.
        .filter(
            F.col("_ingested_at") >= F.lit(watermark)
            if watermark is not None and not gold_needs_backfill else F.lit(True)
        )
    )

    new_watermark = (
        df_silver_incremental
        .agg(F.max("_ingested_at").alias("max_ingested_at"))
        .collect()[0]["max_ingested_at"]
    )
    has_new_rows = new_watermark is not None
    print("New Silver rows for Gold:", has_new_rows)



    affected_daily = (
        df_silver_incremental
        .select("symbol", "trade_date")
        .distinct()
    )



    affected_hourly = (
        df_silver_incremental
        .withColumn(
            "trade_hour",
            F.hour("trade_timestamp")
        )
        .select("symbol", "trade_date", "trade_hour")
        .distinct()
    )




    if has_new_rows:

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

        daily_merge = (
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
        )
        if full_rebuild:
            daily_merge = daily_merge.whenNotMatchedBySourceDelete()
        daily_merge.execute()

        print("Daily Gold MERGE completed.")

    else:
        if full_rebuild:
            spark.sql(f"DELETE FROM {DAILY_GOLD_TABLE}")
        print("No Silver rows to aggregate for Daily Gold.")


    if has_new_rows:

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

        hourly_merge = (
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
        )
        if full_rebuild:
            hourly_merge = hourly_merge.whenNotMatchedBySourceDelete()
        hourly_merge.execute()

        print("Hourly Gold MERGE completed.")

    else:
        if full_rebuild:
            spark.sql(f"DELETE FROM {HOURLY_GOLD_TABLE}")
        print("No Silver rows to aggregate for Hourly Gold.")


    if has_new_rows:
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

    if full_rebuild:
        for table_name in (DAILY_GOLD_TABLE, HOURLY_GOLD_TABLE):
            spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES ('finnhub.gold.schema_version' = '{GOLD_SCHEMA_VERSION}')
            """)
        print("Finnhub Gold event-ID / UTC migration completed.")

acquire_lock()
try:
    run_gold()
finally:
    release_lock()
