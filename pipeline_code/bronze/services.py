TABLE_NAME = "services.bronze.services"
SOURCE_PATH = "/Volumes/services/bronze/services"

spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

spark.sql(f"""
    COPY INTO {TABLE_NAME}
    FROM (
        SELECT
            *,
            _metadata.file_path AS file_name,
            current_timestamp() AS ingest_datetime
        FROM '{SOURCE_PATH}'
    )
    FILEFORMAT = CSV
    FORMAT_OPTIONS (
        'header' = 'true',
        'inferSchema' = 'true',
        'mergeSchema' = 'true',
        'mode' = 'PERMISSIVE'
    )
    COPY_OPTIONS (
        'mergeSchema' = 'true'
    )
""")
