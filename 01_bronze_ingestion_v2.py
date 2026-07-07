import os
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, col

# ====================================================================
# 1. THE DELTA CONTROL ENGINE & CONFIGURATION (V2)
# ====================================================================
print("V2 BRONZE OMNI-INGESTION: DELTA CONTROL TABLE MODE\n")

# Fully isolated V2 paths
landing_zone = "/Volumes/workspace/default/landing_zone_v2/"
bronze_table_zone = "/Volumes/workspace/default/landing_zone_v2/archive/"
raw_file_archive = "/Volumes/workspace/default/landing_zone_v2/raw_archive/"
dbutils.fs.mkdirs(raw_file_archive) 

audit_table = "workspace.default.pipeline_audit_logs_v2"

# Fetch routing rules directly from the V2 Delta Database
df_config = spark.read.table("workspace.default.pipeline_configuration_v2") \
                 .filter("is_active = true") \
                 .orderBy("process_priority")

tables_config = [row.asDict(recursive=True) for row in df_config.collect()]

# Scan the V2 Landing Zone
try:
    landing_files = [
        f.path
        for f in dbutils.fs.ls(landing_zone)
        if not (
            "archive" in f.path.lower()
            or "raw_archive" in f.path.lower()
        )
    ]

except Exception as e:
    print("Landing zone is empty or missing.")
    landing_files = []

if not landing_files:
    print("No files detected in V2 Landing Zone. Pipeline standing by.")

# ====================================================================
# 2. V2 BRONZE PROCESSING ENGINE
# ====================================================================
for table_rules in tables_config:
    target_name = table_rules.get("target_table")
    file_pattern = table_rules.get("file_pattern")
    
    matched_files = [f for f in landing_files if file_pattern.lower() in f.lower()]
    
    if not matched_files:
        continue

    print(f"\n>>> Priority Processing Triggered for Target: {target_name}")

    for source_path in matched_files:
        file_name = source_path.rstrip("/").split("/")[-1]
        file_extension = file_name.split(".")[-1].lower()
        
        uc_source_path = f"{landing_zone}{file_name}"
        uc_archive_path = f"{raw_file_archive}{target_name}_{file_name}"
        
        print(f"--- Found File: {file_name} ---")
        start_time = datetime.now()
        status, error_message, rows_processed = "Failed", "None", 0
        
        try:
            df_raw = None
            
            if file_extension == "csv":
                df_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(uc_source_path)
            

            elif file_extension == "json":
                df_raw = (
                    spark.read
                    .option("multiline", "true")
                    .json(uc_source_path)
                )

                if "_corrupt_record" in df_raw.columns:
                    raise ValueError(
                        f"Invalid JSON structure in {file_name}"
                    )
            
            elif file_extension == "xml":
                expected_tag = "order" if "order" in target_name.lower() else "employee"
                xml_row_tag = table_rules.get("row_tag", expected_tag)
                df_raw = spark.read.format("xml").option("rowTag", xml_row_tag).option("inferSchema", "true").load(uc_source_path)
            elif file_extension == "parquet":
                df_raw = spark.read.format("parquet").load(uc_source_path)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")

            # Attach metadata
            df_bronze = df_raw.withColumn("source_file", lit(file_name)) \
                              .withColumn("ingest_timestamp", current_timestamp())

            # THE FIX: The String-Safe Bronze Cast
            # This guarantees that CSV, JSON, and XML columns never clash in Parquet
            for c in df_bronze.columns:
                df_bronze = df_bronze.withColumn(c, col(c).cast("string"))

            # 1. Save to V2 Bronze Table Archive FIRST
            df_bronze.write.format("parquet").mode("append").save(f"{bronze_table_zone}{target_name}/")
            
            # 2. Count the rows SECOND
            rows_processed = df_bronze.count()
            
            # 3. Move the raw file to the RAW archive LAST
            dbutils.fs.mv(uc_source_path, uc_archive_path)
            
            status = "Success"
            print("Success! Data archived.")

        except Exception as e:
            error_message = str(e)
            print(f"Pipeline failure for {file_name}: {error_message[:100]}")
            
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            clean_err = error_message.replace("'", "''")
            
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {audit_table} (
                    table_name STRING, layer STRING, start_time TIMESTAMP, 
                    end_time TIMESTAMP, duration_seconds DOUBLE, 
                    rows_processed LONG, status STRING, error_message STRING
                )
            """)
            
            spark.sql(f"""
                INSERT INTO {audit_table} VALUES (
                    '{target_name}', 'Bronze_V2', '{start_time.strftime('%Y-%m-%d %H:%M:%S')}', 
                    '{end_time.strftime('%Y-%m-%d %H:%M:%S')}', {duration}, 
                    {rows_processed}, '{status}', '{clean_err}'
                )
            """)