from pyspark import pipelines as dp
import pyspark.sql.functions as F
import re

SOURCE_PATH = "/Volumes/services/bronze/services"

def sanitize_column_name(name):
    """Convert column names to safe lowercase with underscores."""
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower()

@dp.table(
    name="services.bronze.services",
    comment="Streaming ingestion of raw services data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def services_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )

    # Sanitize column names
    for col_name in df.columns:
        safe_name = sanitize_column_name(col_name)
        if col_name != safe_name:
            df = df.withColumnRenamed(col_name, safe_name)

    # Add metadata columns
    df = df.withColumn("file_name", F.col("_metadata.file_path"))
    df = df.withColumn("ingest_datetime", F.current_timestamp())

    return df