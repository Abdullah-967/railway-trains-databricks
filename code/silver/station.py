from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="services.silver.station",
    comment="Cleaned and standardized station data with geo coordinates and classification flags",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def station_silver():
    """
    Clean and enrich station master data.

    Transformations:
    - Column aliasing and selection
    - Cast geo coordinates to DOUBLE
    - Derive classification flags (is_junction, is_intercity, is_major_hub, is_dutch)

    Grain: One row per station
    """
    df_bronze = spark.read.table("services.bronze.station")

    df_silver = df_bronze.select(
        # Core identifiers
        F.col("code").alias("station_code"),
        F.col("name_long").alias("station_name"),
        F.col("name_short").alias("station_name_short"),
        F.col("name_medium").alias("station_name_medium"),
        F.col("country").alias("station_country"),
        F.col("type").alias("station_type"),
        F.col("uic").alias("station_uic"),
        F.col("slug").alias("station_slug"),

        # Geo coordinates - cast to double
        F.col("geo_lat").cast("double").alias("station_lat"),
        F.col("geo_lng").cast("double").alias("station_lng"),

        # Derived flags
        F.col("type").contains("knooppunt").alias("is_junction"),
        F.col("type").isin("megastation", "knooppuntIntercitystation", "intercitystation").alias("is_intercity"),
        F.when(F.col("type") == "megastation", True).otherwise(False).alias("is_major_hub"),
        F.when(F.col("country") == "NL", True).otherwise(False).alias("is_dutch"),

        # Metadata
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    # Add processed timestamp
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    return df_silver