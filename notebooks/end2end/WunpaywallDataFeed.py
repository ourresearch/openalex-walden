# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime, timedelta
import os
import json

# COMMAND ----------

dbutils.widgets.dropdown("mode", "daily", ["daily", "weekly"], "Export Mode")
mode = dbutils.widgets.get("mode")
print(f"mode is {mode}")

last_export_table = f"openalex.unpaywall.last_{mode}_export_timestamp"
metadata_table = "openalex.unpaywall.export_metadata"
s3_bucket = "unpaywall-data-feed-walden"
s3_prefix = mode
source_table = "openalex.unpaywall.unpaywall"

# COMMAND ----------

def get_last_run_timestamp():
    """
    Retrieve the timestamp of when this job was last run
    Returns timestamp string in the format used by the source table
    """
    table_exists = False
    min_timestamp_str = '2025-05-17T04:11:03.746+00:00'

    try:
        # Handle the timezone offset in the minimum timestamp
        min_timestamp_str_clean = min_timestamp_str.replace("+00:00", "Z")
        min_timestamp = datetime.strptime(min_timestamp_str_clean, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        # Alternative format if the first parsing fails
        min_timestamp = datetime.strptime(min_timestamp_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    
    # Format the minimum timestamp in the source table format
    min_formatted_timestamp = min_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')

    try:
        spark.sql(f"DESCRIBE TABLE {last_export_table}")
        table_exists = True
    except:
        print(f"Metadata table {last_export_table} does not exist yet - will create it on first run")
    
    if table_exists:
        try:
            df = spark.sql(f"SELECT timestamp FROM {last_export_table} ORDER BY run_id DESC LIMIT 1")
            if df.count() > 0:
                # Get the stored timestamp
                stored_timestamp = df.collect()[0]['timestamp']
                
                # Convert from ISO format to the format used in the source table
                try:
                    dt = datetime.strptime(stored_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
                    if dt < min_timestamp:
                        print(f"Stored timestamp {stored_timestamp} is earlier than minimum allowed timestamp. Using minimum timestamp.")
                        return min_formatted_timestamp
                    # Format it like: '2024-12-29 23:55:21.079000'
                    formatted_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                    print(f"Converted timestamp from {stored_timestamp} to {formatted_timestamp}")
                    return formatted_timestamp
                except Exception as e:
                    print(f"Error converting timestamp format: {e}")
                    # Return the original if conversion fails
                    return stored_timestamp
        except Exception as e:
            print(f"Error reading last run timestamp: {e}")
    
    # For the default timestamp, use the correct format directly
    default_timestamp = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(f"No previous timestamp found, defaulting to {default_timestamp}")
    return default_timestamp

def save_current_run_timestamp():
    """
    Save the current timestamp as a new row in the metadata table.
    """
    current_time_iso = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    run_id = datetime.utcnow().strftime('%Y%m%dT%H%M%S')

    last_export_df = spark.createDataFrame([
        (run_id, current_time_iso)
    ], schema=StructType([
        StructField("run_id", StringType(), False),
        StructField("timestamp", StringType(), False)
    ]))

    last_export_df.write.mode("append").format("delta").saveAsTable(last_export_table)
    
    print(f"Saved run last export data: run_id={run_id}, timestamp={current_time_iso}")
    
    current_time_formatted = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    return current_time_formatted

last_run_timestamp = get_last_run_timestamp()
print(f"Last run timestamp: {last_run_timestamp}")

if mode == "weekly":
    from_dt = datetime.strptime(last_run_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    to_dt = datetime.utcnow()
    from_date = from_dt.date()
    to_date = to_dt.date()
else:
    from_dt = None
    to_dt = datetime.utcnow()
    from_date = None
    to_date = None

# COMMAND ----------

df = spark.table(source_table).filter(F.col("updated_date") > last_run_timestamp)
record_count = df.count()
print(f"Found {record_count} records updated since {last_run_timestamp}")

# safety measure
if record_count > 1500000:
    print("Too many records found to export. Need to run manually")
    dbutils.notebook.exit("Too many records found to export: {record_count:,}")

# COMMAND ----------

if record_count == 0:
    print("No records found to export. Job completed.")
    dbutils.notebook.exit("No records found to export")

current_date = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')

if mode == "weekly":
    from_dt_str = from_dt.strftime("%Y-%m-%dT%H%M%S")
    to_dt_str = to_dt.strftime("%Y-%m-%dT%H%M%S")
    filename = f"changed_dois_with_versions_{from_dt_str}_to_{to_dt_str}.jsonl.gz"
else:
    to_dt_str = to_dt.strftime("%Y-%m-%dT%H%M%S")
    filename = f"changed_dois_with_versions_{to_dt_str}.jsonl.gz"

final_path = f"s3://{s3_bucket}/{s3_prefix}/{filename}"

print(f"Exporting {record_count} records to {final_path}")

# Write the data using Spark's parallelism, but ensure we get a single file output
temp_dir = f"s3://{s3_bucket}/{s3_prefix}temp_export_{current_date}/"

partition_count = max(1, int(record_count / 500000))  # Assuming ~256 bytes per record

(df.select("json_response")
    .repartition(partition_count)
    .write
    .format("text")
    .mode("overwrite")
    .option("compression", "gzip")
    .save(temp_dir)
)

# Get all the part files
part_files = [f.path for f in dbutils.fs.ls(temp_dir) if f.name.startswith("part-") and f.name.endswith(".gz")]

# If there's just one part file, rename it
if len(part_files) == 1:
    single_part_file = part_files[0]
    dbutils.fs.cp(part_files[0], final_path)
else:
    # Create a temporary merged file in DBFS
    merged_temp_path = "/dbfs/tmp/unpaywall_merged.jsonl.gz"
    
    # Use Spark to read all part files and write as a single file
    spark.read.text(temp_dir).coalesce(1).write.format("text").mode("overwrite").option("compression", "gzip").save("/tmp/unpaywall_single/")
    
    # Get the resulting single part file
    single_part_file = [f.path for f in dbutils.fs.ls("/tmp/unpaywall_single/") if f.name.startswith("part-") and f.name.endswith(".gz")][0]
    
    # Copy to the final destination with proper name
    dbutils.fs.cp(single_part_file, final_path)
    
    # Clean up temporary files
    dbutils.fs.rm("/tmp/unpaywall_single/", recurse=True)

# Clean up the temporary directory
dbutils.fs.rm(temp_dir, recurse=True)

print(f"Successfully exported {record_count} records to {final_path}")

# Update the last run timestamp
new_timestamp = save_current_run_timestamp()
print(f"Updated job metadata with new timestamp: {new_timestamp}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metadata_table} (
  export_timestamp STRING,
  mode STRING,
  file_name STRING,
  file_path STRING,
  file_size_bytes BIGINT,
  line_count INT,
  from_date DATE,
  to_date DATE
) USING DELTA
""")

metadata_schema = StructType([
    StructField("export_timestamp", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("file_size_bytes", LongType(), True),
    StructField("line_count", IntegerType(), True),
    StructField("from_date", DateType(), True),
    StructField("to_date", DateType(), True)
])


# get file size (in bytes)
try:
    file_info = dbutils.fs.ls(final_path)[0]
    file_size_bytes = file_info.size
    print(f"File size: {file_size_bytes} bytes")
except Exception as e:
    print(f"Error getting file size: {e}")
    file_size_bytes = 0

print(f"File size: {file_size_bytes} bytes")

metadata_row = Row(
    export_timestamp=new_timestamp,
    mode=mode,
    file_name=filename,
    file_path=final_path,
    file_size_bytes=file_size_bytes,
    line_count=record_count,
    from_date=from_date,
    to_date=to_date
)

metadata_df = spark.createDataFrame([metadata_row], schema=metadata_schema)
metadata_df.write.format("delta").mode("append").saveAsTable(metadata_table)

print("Job execution summary:")
print(f"- Source table: {source_table}")
print(f"- Last run timestamp: {last_run_timestamp}")
print(f"- Lines exported: {record_count}")

# COMMAND ----------

print("Syncing metadata table to PostgreSQL...")

%pip install sqlalchemy psycopg2-binary

from sqlalchemy import create_engine, text
import time

user = dbutils.secrets.get(scope="postgres-works", key="user")
password = dbutils.secrets.get(scope="postgres-works", key="password")
pg_metadata_table = "unpaywall.export_metadata"
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres")

df = spark.table(metadata_table)

# Create the target table in PostgreSQL if it doesn't exist
with engine.connect() as conn:
    conn.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {pg_metadata_table} (
      export_timestamp VARCHAR(255),
      mode VARCHAR(50),
      file_name VARCHAR(255),
      file_path VARCHAR(500),
      file_size_bytes BIGINT,
      line_count INT,
      from_date DATE,
      to_date DATE,
      PRIMARY KEY (export_timestamp, mode, file_name)
    );
    """))
    conn.commit()

temp_table = f"{pg_metadata_table}_temp_{int(time.time())}"

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres") \
    .option("dbtable", temp_table) \
    .option("user", user) \
    .option("password", password) \
    .option("batchsize", 100) \
    .option("numPartitions", 4) \
    .mode("overwrite") \
    .save()

# merge the temporary table into the main table
with engine.connect() as conn:
    with conn.begin():
        # Use TRUNCATE + INSERT for a full sync approach
        conn.execute(text(f"""
        TRUNCATE TABLE {pg_metadata_table};
        
        INSERT INTO {pg_metadata_table} (
            export_timestamp, mode, file_name, file_path, 
            file_size_bytes, line_count, from_date, to_date
        )
        SELECT 
            export_timestamp, mode, file_name, file_path, 
            file_size_bytes, line_count, from_date, to_date
        FROM {temp_table};
        """))
        
        # clean up the temporary table
        conn.execute(text(f"DROP TABLE {temp_table};"))

print(f"Successfully synced {df.count()} metadata records to PostgreSQL")
