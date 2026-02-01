from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = "/Volumes/services/bronze/stations"

@dp.materialized_view(
    name="services.bronze.station",
    comment="Stations Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def station_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("mergeSchema", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )

    # Add metadata columns
    df = df.withColumn("file_name", col("_metadata.file_path"))
    df = df.withColumn("ingest_datetime", current_timestamp())

    return df
