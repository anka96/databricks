import dlt
from pyspark.sql.functions import col, struct

# Note: Since Parquet files have a built-in schema, 
# we don't need the manual payload_schema unless your 
# Parquet data contains JSON strings inside a column.

@dlt.table(
    name="telematics_advanced",
    comment="Parquet files ingested via Auto Loader into Bronze",
    table_properties={"quality": "bronze"}
)
def bronze_table():
    # Use 'cloudFiles' to automatically ingest files from a directory
    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        # Ensure your S3/DBFS path is correct and accessible
        .load("/Workspace/Users/kryshtopenko@gmail.com/databricks/data/telematics/part-00000-tid-2652813375822267916-d5fd72d6-838a-486e-9d8c-4990b298f394-299641-1-c000.snappy.parquet") 
    )