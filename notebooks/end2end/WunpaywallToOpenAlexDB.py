# Databricks notebook source
# MAGIC %pip install sqlalchemy
# MAGIC
# MAGIC from sqlalchemy import create_engine, text
# MAGIC from sqlalchemy.pool import QueuePool
# MAGIC from datetime import datetime, timedelta
# MAGIC import time
# MAGIC
# MAGIC # get the target table name and connection details
# MAGIC user = dbutils.secrets.get(scope = "postgres-works", key = "user")
# MAGIC password = dbutils.secrets.get(scope = "postgres-works", key = "password")
# MAGIC table_name = "unpaywall.unpaywall_from_walden"
# MAGIC RDS_URL = "openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres"
# MAGIC engine = create_engine(
# MAGIC     f"postgresql+psycopg2://{user}:{password}@{RDS_URL}",
# MAGIC     pool_pre_ping=True,
# MAGIC     connect_args={
# MAGIC         "keepalives": 1,
# MAGIC         "keepalives_idle": 30,
# MAGIC         "keepalives_interval": 10,
# MAGIC         "keepalives_count": 5,
# MAGIC         "options": "-c statement_timeout=600000",  # 10 min per statement
# MAGIC     },
# MAGIC )
# MAGIC
# MAGIC # safety to require manual override of large record changes
# MAGIC LARGE_RECORD_COUNT = 25000000
# MAGIC # batch size for merge operations (smaller = less memory, more reliable connections)
# MAGIC MERGE_BATCH_SIZE = 50000
# MAGIC MAX_BATCH_RETRIES = 3
# MAGIC dbutils.widgets.dropdown("force_large_load", "false", ["false", "true"], "Force Large Load Override")
# MAGIC force_large_load = dbutils.widgets.get("force_large_load").lower() == "true"
# MAGIC
# MAGIC # check if this is the first run by seeing if the table exists and has data
# MAGIC with engine.connect() as conn:
# MAGIC     result = conn.execute(text(f"""
# MAGIC         SELECT EXISTS (
# MAGIC             SELECT FROM information_schema.tables
# MAGIC             WHERE table_schema = 'unpaywall'
# MAGIC             AND table_name = 'unpaywall_from_walden'
# MAGIC         ) AND EXISTS (
# MAGIC             SELECT 1 FROM {table_name} LIMIT 1
# MAGIC         );
# MAGIC     """))
# MAGIC     table_exists_with_data = result.scalar()
# MAGIC
# MAGIC     if table_exists_with_data:
# MAGIC         # Check when the last update occurred
# MAGIC         result = conn.execute(text(f"SELECT MAX(updated_date) FROM {table_name};"))
# MAGIC         do_full_load = False
# MAGIC         last_updated = result.scalar()
# MAGIC     else:
# MAGIC         # Table doesn't exist or is empty, definitely do a full load
# MAGIC         do_full_load = True
# MAGIC         last_updated = None
# MAGIC
# MAGIC if do_full_load:
# MAGIC     print("Performing full load")
# MAGIC
# MAGIC     # read the full dataset from Databricks
# MAGIC     df = spark.read.table("openalex.unpaywall.unpaywall") \
# MAGIC         .select("doi", "json_response", "updated_date") \
# MAGIC         .dropDuplicates(["doi"])
# MAGIC
# MAGIC     record_count = df.count()
# MAGIC     print(f"Total records to load: {record_count}")
# MAGIC
# MAGIC     # if table exists, truncate it; otherwise create it
# MAGIC     with engine.connect() as conn:
# MAGIC         result = conn.execute(text(f"""
# MAGIC             SELECT EXISTS (
# MAGIC                 SELECT FROM information_schema.tables
# MAGIC                 WHERE table_schema = 'unpaywall'
# MAGIC                 AND table_name = 'unpaywall_from_walden'
# MAGIC             );
# MAGIC         """))
# MAGIC         table_exists = result.scalar()
# MAGIC
# MAGIC         if table_exists:
# MAGIC             print(f"Truncating existing table {table_name}")
# MAGIC             conn.execute(text(f"TRUNCATE TABLE {table_name};"))
# MAGIC         else:
# MAGIC             print(f"Creating new table {table_name}")
# MAGIC             conn.execute(text(f"""
# MAGIC                 CREATE TABLE {table_name} (
# MAGIC                     doi TEXT PRIMARY KEY,
# MAGIC                     json_response JSONB,
# MAGIC                     updated_date TIMESTAMP
# MAGIC                 );
# MAGIC             """))
# MAGIC
# MAGIC     # start the load timer
# MAGIC     start_time = time.time()
# MAGIC
# MAGIC     # write directly to the target table
# MAGIC     df.write \
# MAGIC         .format("jdbc") \
# MAGIC         .option("url", f"jdbc:postgresql://{RDS_URL}") \
# MAGIC         .option("dbtable", table_name) \
# MAGIC         .option("user", user) \
# MAGIC         .option("password", password) \
# MAGIC         .option("batchsize", 1000)  \
# MAGIC         .option("numPartitions", 120) \
# MAGIC         .mode("append") \
# MAGIC         .save()
# MAGIC
# MAGIC     elapsed_time = time.time() - start_time
# MAGIC     records_per_second = record_count / elapsed_time
# MAGIC     print(f"Full load completed in {elapsed_time:.2f} seconds ({records_per_second:.2f} records/second)")
# MAGIC
# MAGIC else:
# MAGIC     print(f"Performing incremental load. Loading records updated after {last_updated}")
# MAGIC
# MAGIC     # get only records that are newer than the latest in PostgreSQL
# MAGIC     df = spark.read.table("openalex.unpaywall.unpaywall") \
# MAGIC         .select("doi", "json_response", "updated_date") \
# MAGIC         .filter(f"updated_date > '{last_updated}'") \
# MAGIC         .dropDuplicates(["doi"])
# MAGIC
# MAGIC     record_count = df.count()
# MAGIC     print(f"Found {record_count} records to update")
# MAGIC
# MAGIC     # safety
# MAGIC     if record_count > LARGE_RECORD_COUNT and not force_large_load:
# MAGIC         error_msg = f"Too many records found to export ({record_count:,}). Set 'Force Large Load Override' to 'true' to proceed anyway, or run manually"
# MAGIC         print(error_msg)
# MAGIC         raise Exception(error_msg)
# MAGIC     elif record_count > LARGE_RECORD_COUNT and force_large_load:
# MAGIC         print(f"WARNING: Processing large dataset ({record_count:,} records) due to override being enabled")
# MAGIC
# MAGIC     if record_count > 0:
# MAGIC         # create a temporary table for the new/updated records
# MAGIC         temp_table = f"{table_name}_temp_{int(time.time())}"
# MAGIC
# MAGIC         df.write \
# MAGIC             .format("jdbc") \
# MAGIC             .option("url", f"jdbc:postgresql://{RDS_URL}") \
# MAGIC             .option("dbtable", temp_table) \
# MAGIC             .option("user", user) \
# MAGIC             .option("password", password) \
# MAGIC             .option("batchsize", 1000) \
# MAGIC             .option("numPartitions", 120) \
# MAGIC             .mode("overwrite") \
# MAGIC             .save()
# MAGIC
# MAGIC         print(f"Created temporary table {temp_table}, merging in batches of {MERGE_BATCH_SIZE:,}")
# MAGIC
# MAGIC         # add index on temp table for efficient batched reads
# MAGIC         with engine.connect() as conn:
# MAGIC             with conn.begin():
# MAGIC                 conn.execute(text(f"CREATE INDEX ON {temp_table} (doi);"))
# MAGIC         print("Created index on temp table")
# MAGIC
# MAGIC         # get total count and calculate batches
# MAGIC         with engine.connect() as conn:
# MAGIC             result = conn.execute(text(f"SELECT COUNT(*) FROM {temp_table};"))
# MAGIC             temp_count = result.scalar()
# MAGIC
# MAGIC         num_batches = (temp_count + MERGE_BATCH_SIZE - 1) // MERGE_BATCH_SIZE
# MAGIC         print(f"Will process {temp_count:,} records in {num_batches} batches")
# MAGIC
# MAGIC         merge_start = time.time()
# MAGIC         total_merged = 0
# MAGIC
# MAGIC         # merge in batches using OFFSET/LIMIT with ordered reads
# MAGIC         for batch_num in range(num_batches):
# MAGIC             batch_start = time.time()
# MAGIC             offset = batch_num * MERGE_BATCH_SIZE
# MAGIC
# MAGIC             for attempt in range(1, MAX_BATCH_RETRIES + 1):
# MAGIC                 try:
# MAGIC                     with engine.connect() as conn:
# MAGIC                         with conn.begin():
# MAGIC                             result = conn.execute(text(f"""
# MAGIC                                 INSERT INTO {table_name} (doi, json_response, updated_date)
# MAGIC                                 SELECT doi, json_response, updated_date
# MAGIC                                 FROM {temp_table}
# MAGIC                                 ORDER BY doi
# MAGIC                                 LIMIT {MERGE_BATCH_SIZE} OFFSET {offset}
# MAGIC                                 ON CONFLICT (doi) DO UPDATE SET
# MAGIC                                     json_response = EXCLUDED.json_response,
# MAGIC                                     updated_date = EXCLUDED.updated_date
# MAGIC                                 WHERE {table_name}.updated_date < EXCLUDED.updated_date
# MAGIC                                 OR {table_name}.updated_date IS NULL;
# MAGIC                             """))
# MAGIC                             rows_affected = result.rowcount
# MAGIC                     break  # success
# MAGIC                 except Exception as e:
# MAGIC                     if attempt < MAX_BATCH_RETRIES:
# MAGIC                         wait = 30 * attempt
# MAGIC                         print(f"Batch {batch_num + 1} attempt {attempt} failed: {e}. Retrying in {wait}s...")
# MAGIC                         time.sleep(wait)
# MAGIC                         engine.dispose()  # force new connections
# MAGIC                     else:
# MAGIC                         print(f"Batch {batch_num + 1} failed after {MAX_BATCH_RETRIES} attempts")
# MAGIC                         raise
# MAGIC
# MAGIC             total_merged += rows_affected
# MAGIC             batch_elapsed = time.time() - batch_start
# MAGIC             overall_elapsed = time.time() - merge_start
# MAGIC             rate = total_merged / overall_elapsed if overall_elapsed > 0 else 0
# MAGIC             eta = (temp_count - total_merged) / rate if rate > 0 else 0
# MAGIC
# MAGIC             print(f"Batch {batch_num + 1}/{num_batches}: merged {rows_affected:,} rows in {batch_elapsed:.1f}s "
# MAGIC                   f"(total: {total_merged:,}, rate: {rate:,.0f} rows/s, ETA: {eta/60:.0f}m)")
# MAGIC
# MAGIC         # clean up the temporary table
# MAGIC         with engine.connect() as conn:
# MAGIC             with conn.begin():
# MAGIC                 conn.execute(text(f"DROP TABLE {temp_table};"))
# MAGIC
# MAGIC         total_elapsed = time.time() - merge_start
# MAGIC         print(f"Merge completed: {total_merged:,} records in {total_elapsed:.1f}s ({total_merged/total_elapsed:,.0f} rows/s)")
# MAGIC     else:
# MAGIC         print("No new or updated records to process")
