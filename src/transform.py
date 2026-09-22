import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StockTransform")
        .master("local[*]")
        .getOrCreate()
    )


def transform():
    spark = get_spark()
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    raw_files = glob.glob(os.path.join(RAW_DIR, "*.parquet"))
    if not raw_files:
        print("No raw files found. Run ingest.py first.")
        return

    df = spark.read.parquet(*raw_files)

    # Drop exact duplicate rows (same symbol + date)
    df = df.dropDuplicates(["symbol", "date"])

    # Keep only rows with valid prices
    df = df.filter(
        (F.col("close").isNotNull()) & (F.col("close") > 0)
    )

    # Window: one partition per stock, ordered by date
    w = Window.partitionBy("symbol").orderBy("date")

    df = df.withColumn("prev_close", F.lag("close").over(w))
    df = df.withColumn(
        "daily_return_pct",
        F.round(((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100, 2),
    )
    df = df.withColumn(
        "moving_avg_7d",
        F.round(F.avg("close").over(w.rowsBetween(-6, 0)), 2),
    )
    df = df.withColumn(
        "moving_avg_30d",
        F.round(F.avg("close").over(w.rowsBetween(-29, 0)), 2),
    )
    df = df.withColumn(
        "volatility_7d",
        F.round(F.stddev("daily_return_pct").over(w.rowsBetween(-6, 0)), 2),
    )

    # Flag abnormal single-day price jumps (>15%) for the data quality step
    df = df.withColumn(
        "is_price_jump_flag",
        F.when(F.abs(F.col("daily_return_pct")) > 15, True).otherwise(False),
    )

    out_path = os.path.join(PROCESSED_DIR, "prices_transformed.parquet")
    df.write.mode("overwrite").parquet(out_path)

    print(f"Transformed {df.count()} rows -> {out_path}")
    df.select(
        "symbol", "date", "close", "daily_return_pct", "moving_avg_7d", "moving_avg_30d"
    ).show(10)

    spark.stop()


if __name__ == "__main__":
    transform()