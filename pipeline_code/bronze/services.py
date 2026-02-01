from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
import re

SOURCE_PATH = "/Volumes/services/bronze/services"

def sanitize_column_name(name):
    """Convert column names to safe lowercase with underscores."""
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower()

@dp.materialized_view(
    name="services.bronze.services",
    comment="Batch ingestion of raw services data",
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
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("mergeSchema", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )

    # Sanitize column names
    for col_name in df.columns:
        safe_name = sanitize_column_name(col_name)
        if col_name != safe_name:
            df = df.withColumnRenamed(col_name, safe_name)

    # Add metadata columns
    df = df.withColumn("file_name", col("_metadata.file_path"))
    df = df.withColumn("ingest_datetime", current_timestamp())

    return df
