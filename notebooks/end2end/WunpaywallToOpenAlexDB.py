# Databricks notebook source
# MAGIC %pip install sqlalchemy
# MAGIC
# MAGIC from sqlalchemy import create_engine, text
# MAGIC from datetime import datetime, timedelta
# MAGIC import time
# MAGIC
# MAGIC # get the target table name and connection details
# MAGIC user = dbutils.secrets.get(scope = "postgres-works", key = "user")
# MAGIC password = dbutils.secrets.get(scope = "postgres-works", key = "password")
# MAGIC table_name = "unpaywall.unpaywall_from_walden"
# MAGIC engine = create_engine(f"postgresql+psycopg2://{user}:{password}@openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres")
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
# MAGIC         .option("url", "jdbc:postgresql://openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres") \
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
# MAGIC     if record_count > 1500000:
# MAGIC         error_msg = f"Too many records found to export ({record_count:,}). Need to run manually"
# MAGIC         print(error_msg)
# MAGIC         raise Exception(error_msg)
# MAGIC     
# MAGIC     if record_count > 0:
# MAGIC         # create a temporary table for the new/updated records
# MAGIC         temp_table = f"{table_name}_temp_{int(time.time())}"
# MAGIC         
# MAGIC         df.write \
# MAGIC             .format("jdbc") \
# MAGIC             .option("url", "jdbc:postgresql://openalex-1.cqlclvdbbujw.us-east-1.rds.amazonaws.com:5432/postgres") \
# MAGIC             .option("dbtable", temp_table) \
# MAGIC             .option("user", user) \
# MAGIC             .option("password", password) \
# MAGIC             .option("batchsize", 1000) \
# MAGIC             .option("numPartitions", 120) \
# MAGIC             .mode("overwrite") \
# MAGIC             .save()
# MAGIC         
# MAGIC         print(f"Created temporary table {temp_table}, merging")
# MAGIC         
# MAGIC         # merge data from temporary table to the main table
# MAGIC         with engine.connect() as conn:
# MAGIC             with conn.begin():
# MAGIC                 conn.execute(text(f"""
# MAGIC                     INSERT INTO {table_name} (doi, json_response, updated_date)
# MAGIC                     SELECT doi, json_response, updated_date
# MAGIC                     FROM {temp_table}
# MAGIC                     ON CONFLICT (doi) DO UPDATE SET
# MAGIC                         json_response = EXCLUDED.json_response,
# MAGIC                         updated_date = EXCLUDED.updated_date
# MAGIC                     WHERE {table_name}.updated_date < EXCLUDED.updated_date
# MAGIC                     OR {table_name}.updated_date IS NULL;
# MAGIC                 """))
# MAGIC                 
# MAGIC                 # clean up the temporary table
# MAGIC                 conn.execute(text(f"DROP TABLE {temp_table};"))
# MAGIC                 
# MAGIC         print(f"Processed {record_count} records")
# MAGIC     else:
# MAGIC         print("No new or updated records to process")
