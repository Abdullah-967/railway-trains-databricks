from pyspark import pipelines as dp
from pyspark.sql import functions as F


def get_day_name(date_col):
    """Extract day name (Monday, Tuesday, etc.) from a date column."""
    return F.date_format(date_col, "EEEE")


@dp.materialized_view(
    name="services.silver.services",
    comment="Cleaned and validated services with typed delays, boolean flags, and derived on-time metrics.",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
    partition_cols=["service_year", "service_month"],
)
@dp.expect("valid_actual_platform", "stop_actual_platform IS NOT NULL")
@dp.expect("valid_arrival_time", "stop_arrival_time IS NOT NULL")
@dp.expect("valid_departure_time", "stop_departure_time IS NOT NULL")
def services_silver():
    """
    Clean, type, and enrich the raw services/stops data.

    Transformations:
    - Column aliasing and selection
    - Type casting (delays to INT)
    - Boolean parsing for cancellation/change flags
    - Derived on-time flags (<=5 min threshold per NS standard)
    - Partition columns (year, month)

    Grain: One row per stop per service
    """
    df_bronze = spark.read.table("services.bronze.services")

    # Drop rows with nulls in required columns
    df_silver = df_bronze.dropna(
        subset=["stop_actual_platform", "stop_arrival_time", "stop_departure_time"]
    )

    # Select and alias columns
    df_silver = df_silver.select(
        F.col("service_rdt_id").alias("service_id"),
        F.col("service_type").alias("service_type"),
        F.col("service_company").alias("service_company"),
        F.col("service_train_number").alias("service_train_number"),
        F.col("service_date").alias("service_date"),
        F.col("stop_rdt_id").alias("stop_id"),
        F.col("stop_actual_platform").alias("stop_actual_platform"),
        F.col("stop_planned_platform").alias("stop_planned_platform"),
        F.col("stop_arrival_time").alias("stop_arrival_time"),
        F.col("stop_departure_time").alias("stop_departure_time"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
        F.col("service_completely_cancelled").alias("service_completely_cancelled"),
        F.col("service_partly_cancelled").alias("service_partly_cancelled"),
        F.col("service_maximum_delay").alias("service_maximum_delay"),
        F.col("stop_arrival_delay").alias("stop_arrival_delay"),
        F.col("stop_departure_delay").alias("stop_departure_delay"),
        F.col("stop_station_code").alias("stop_station_code"),
        F.col("stop_station_name").alias("stop_station_name"),
        F.col("stop_arrival_cancelled").alias("stop_arrival_cancelled"),
        F.col("stop_departure_cancelled").alias("stop_departure_cancelled"),
        F.col("stop_platform_change").alias("stop_platform_changed"),
    )

    # Add processed timestamp
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    # Add day name of the service
    df_silver = df_silver.withColumn(
        "service_day_name", get_day_name(F.col("service_date"))
    )

    # Type casting: delays to INT
    df_silver = (
        df_silver
        .withColumn("stop_arrival_delay", F.col("stop_arrival_delay").cast("int"))
        .withColumn("stop_departure_delay", F.col("stop_departure_delay").cast("int"))
        .withColumn("service_maximum_delay", F.col("service_maximum_delay").cast("int"))
    )

    # Boolean casting for cancellation/change flags
    df_silver = (
        df_silver
        .withColumn(
            "service_completely_cancelled",
            F.when(F.lower(F.col("service_completely_cancelled")) == "true", True).otherwise(False)
        )
        .withColumn(
            "service_partly_cancelled",
            F.when(F.lower(F.col("service_partly_cancelled")) == "true", True).otherwise(False)
        )
        .withColumn(
            "stop_arrival_cancelled",
            F.when(F.lower(F.col("stop_arrival_cancelled")) == "true", True).otherwise(False)
        )
        .withColumn(
            "stop_departure_cancelled",
            F.when(F.lower(F.col("stop_departure_cancelled")) == "true", True).otherwise(False)
        )
        .withColumn(
            "stop_platform_changed",
            F.when(F.lower(F.col("stop_platform_changed")) == "true", True).otherwise(False)
        )
    )

    # Derived columns for analytics
    df_silver = (
        df_silver
        .withColumn(
            "is_on_time_arrival",
            F.when(F.col("stop_arrival_cancelled") == True, False)
             .when(F.col("stop_arrival_delay") <= 5, True)
             .otherwise(False)
        )
        .withColumn(
            "is_on_time_departure",
            F.when(F.col("stop_departure_cancelled") == True, False)
             .when(F.col("stop_departure_delay") <= 5, True)
             .otherwise(False)
        )
    )

    # Partition columns
    df_silver = (
        df_silver
        .withColumn("service_year", F.year(F.col("service_date")))
        .withColumn("service_month", F.month(F.col("service_date")))
    )

    return df_silver
