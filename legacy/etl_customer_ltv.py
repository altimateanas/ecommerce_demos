"""
Legacy ETL: etl_customer_ltv
PySpark ETL reading order history from S3, computing customer LTV with groupBy + agg, LAG window for purchase frequency, CASE WHEN for segmentation

Company: CartWave
Industry: Ecommerce

NOTE: This is a legacy PySpark script that predates our dbt migration.
It is kept for reference and as a migration target for the dbt project.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType,
)
from datetime import datetime, timedelta
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etl_customer_ltv")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_INPUT_BUCKET = "s3a://cartwave-raw-data"
S3_OUTPUT_BUCKET = "s3a://cartwave-processed"
PARTITION_COL = "order_date"


def create_spark_session():
    """Create Spark session with appropriate configs."""
    return (
        SparkSession.builder
        .appName("CartWave - etl_customer_ltv")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def read_source_data(spark, table_name, date_filter=None):
    """Read parquet data from S3."""
    path = f"{S3_INPUT_BUCKET}/{table_name}/"
    logger.info(f"Reading from {path}")

    df = spark.read.parquet(path)

    if date_filter:
        df = df.filter(F.col(PARTITION_COL) >= date_filter)
        logger.info(f"  Filtered to {PARTITION_COL} >= {date_filter}: {df.count()} rows")

    return df


def transform_customer(spark, df_primary, df_secondary):
    """
    Main transformation logic for customer data.

    Steps:
      1. Filter active records
      2. Join with secondary table
      3. Aggregate metrics
      4. Apply window functions for ranking
      5. Classify using CASE WHEN logic
    """

    # ---- Step 1: Filter ----
    df_filtered = df_primary.filter(
        (F.col("status") == "active") &
        (F.col("customer_id").isNotNull())
    )
    logger.info(f"After filter: {df_filtered.count()} active records")

    # ---- Step 2: Join ----
    df_joined = df_filtered.join(
        df_secondary,
        on="customer_id",
        how="left"
    )

    # ---- Step 3: Aggregate ----
    df_agg = df_joined.groupBy(
        "customer_id",
        "customer_id",
        "product_category",
    ).agg(
        F.count("*").alias("record_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.min("created_at").alias("first_activity"),
        F.max("created_at").alias("last_activity"),
    )

    # ---- Step 4: Window functions ----
    window_spec = Window.partitionBy("customer_id").orderBy(F.col("total_amount").desc())

    df_ranked = df_agg.withColumn(
        "amount_rank",
        F.row_number().over(window_spec)
    ).withColumn(
        "pct_of_total",
        F.col("total_amount") / F.sum("total_amount").over(
            Window.partitionBy(F.lit(1))
        )
    )

    # ---- Step 5: CASE WHEN classification ----
    df_classified = df_ranked.withColumn(
        "tier",
        F.when(F.col("total_amount") >= 100000, "enterprise")
         .when(F.col("total_amount") >= 10000, "mid_market")
         .when(F.col("total_amount") >= 1000, "small_business")
         .otherwise("starter")
    ).withColumn(
        "is_high_value",
        F.col("total_amount") >= 50000
    )

    return df_classified


def run_spark_sql_analytics(spark, df):
    """
    Run SparkSQL for complex analytics that are easier to express in SQL.
    This embeds SQL directly — a pattern we're migrating to dbt.
    """
    df.createOrReplaceTempView("customer_data")

    result = spark.sql("""
        WITH monthly_metrics AS (
            SELECT
                customer_id,
                DATE_TRUNC('month', last_activity) AS metric_month,
                SUM(total_amount) AS monthly_total,
                COUNT(*) AS monthly_count,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY DATE_TRUNC('month', last_activity) DESC
                ) AS month_rank
            FROM customer_data
            GROUP BY customer_id, DATE_TRUNC('month', last_activity)
        ),

        trends AS (
            SELECT
                customer_id,
                metric_month,
                monthly_total,
                monthly_count,
                LAG(monthly_total) OVER (
                    PARTITION BY customer_id
                    ORDER BY metric_month
                ) AS prev_month_total,
                CASE
                    WHEN LAG(monthly_total) OVER (
                        PARTITION BY customer_id
                        ORDER BY metric_month
                    ) IS NULL THEN 'new'
                    WHEN monthly_total > LAG(monthly_total) OVER (
                        PARTITION BY customer_id
                        ORDER BY metric_month
                    ) * 1.1 THEN 'growing'
                    WHEN monthly_total < LAG(monthly_total) OVER (
                        PARTITION BY customer_id
                        ORDER BY metric_month
                    ) * 0.9 THEN 'declining'
                    ELSE 'stable'
                END AS trend
            FROM monthly_metrics
            WHERE month_rank <= 12
        )

        SELECT
            customer_id,
            metric_month,
            monthly_total,
            prev_month_total,
            trend,
            ROUND(
                CASE
                    WHEN prev_month_total > 0
                    THEN (monthly_total - prev_month_total) / prev_month_total * 100
                    ELSE 0
                END, 2
            ) AS mom_growth_pct
        FROM trends
        ORDER BY customer_id, metric_month
    """)

    return result


def write_output(df, output_name):
    """Write processed data to parquet, partitioned by date."""
    output_path = f"{S3_OUTPUT_BUCKET}/{output_name}/"
    logger.info(f"Writing to {output_path}")

    (
        df
        .repartition(10)
        .write
        .mode("overwrite")
        .partitionBy("metric_month")
        .parquet(output_path)
    )
    logger.info(f"  Written successfully")


def main():
    """Main ETL entry point."""
    logger.info("=" * 60)
    logger.info(f"Starting CartWave Legacy ETL — {datetime.now()}")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Calculate date filter (last 90 days)
        date_filter = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        df_primary = read_source_data(spark, "customers", date_filter)
        df_secondary = read_source_data(spark, "orders", date_filter)

        # Transform
        df_transformed = transform_customer(spark, df_primary, df_secondary)

        # SparkSQL analytics
        df_analytics = run_spark_sql_analytics(spark, df_transformed)

        # Write outputs
        write_output(df_transformed, "customer_summary")
        write_output(df_analytics, "customer_trends")

        row_count = df_transformed.count()
        logger.info(f"ETL complete — {row_count:,} rows processed")

    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
