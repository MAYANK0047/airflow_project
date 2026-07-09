import os
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, lit, row_number

# ====================================================================
# 1. THE DELTA CONTROL ENGINE & CONFIGURATION (V2)
# ====================================================================
print("V2 SILVER DATA QUALITY GATE: DELTA CONTROL TABLE MODE\n")

# Fully isolated V2 paths
bronze_archive = "/Volumes/workspace/default/landing_zone_v2/archive/"
silver_zone = "/Volumes/workspace/default/silver_business_v2/"
quarantine_zone = "/Volumes/workspace/default/quarantine_zone_v2/"
audit_table = "workspace.default.pipeline_audit_logs_v2"

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# Fetch routing rules directly from the V2 Delta Database
df_config = spark.read.table("workspace.default.pipeline_configuration_v2") \
                 .filter("is_active = true") \
                 .orderBy("process_priority")

tables_config = [row.asDict(recursive=True) for row in df_config.collect()]

# --- THE FIX: Pre-scan Bronze Data for Graceful Exit ---
active_tables_with_data = []
for rules in tables_config:
    target_name = rules.get('target_table')
    if path_exists(f"{bronze_archive}{target_name}/"):
        active_tables_with_data.append(rules)

# If no Bronze data exists for ANY table, exit gracefully
if not active_tables_with_data:
    print("No Bronze data exists for any active configuration. Pipeline standing by.")
    
    start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Ensure table exists (defensive programming)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            table_name STRING, layer STRING, start_time TIMESTAMP, 
            end_time TIMESTAMP, duration_seconds DOUBLE, 
            rows_processed LONG, status STRING, error_message STRING
        )
    """)
    
    spark.sql(f"""
        INSERT INTO {audit_table} VALUES (
            'ALL_TARGETS', 'Silver_V2', '{start_time_str}', 
            '{start_time_str}', 0.0, 
            0, 'SKIPPED_NO_FILES', 'None'
        )
    """)
    
    # Force the notebook to stop gracefully and flag as SUCCESS
    dbutils.notebook.exit("SKIP_PIPELINE")

# ====================================================================
# 2. V2 SILVER PROCESSING ENGINE
# ====================================================================
# We now loop over active_tables_with_data instead of tables_config
for rules in active_tables_with_data:
    target_name = rules.get('target_table')
    bronze_path = f"{bronze_archive}{target_name}/"
    silver_target_path = f"{silver_zone}{target_name}/"
    quarantine_target_path = f"{quarantine_zone}{target_name}/"

    print(f"\n--- Running Cleansing Gate for: {target_name} ---")
    start_time = datetime.now()
    status, error_message, rows_processed = "Failed", "None", 0
    
    try:
        # Enable automatic schema merging for multi-format Parquet files
        df_bronze = spark.read.option("mergeSchema", "true").parquet(bronze_path)

        # A. COLUMN STANDARDIZATION
        for c in df_bronze.columns:
            clean_col = c.strip().lower().replace(" ", "_")
            df_bronze = df_bronze.withColumnRenamed(c, clean_col)

        # B. DEDUPLICATION (Keep the latest record based on ingest time)
        natural_keys = rules.get('natural_keys', [])
        if natural_keys:
            window_spec = Window.partitionBy(*[col(k) for k in natural_keys]).orderBy(col("ingest_timestamp").desc())
            df_dedup = df_bronze.withColumn("row_num", row_number().over(window_spec)) \
                                .filter(col("row_num") == 1) \
                                .drop("row_num")
        else:
            df_dedup = df_bronze.dropDuplicates()

        # C. DYNAMIC QUARANTINE ROUTING
        quarantine_rule = rules.get('quarantine_rules')
        df_clean = df_dedup
        
        if quarantine_rule and quarantine_rule.strip().upper() != "NONE":
            df_bad = df_dedup.filter(quarantine_rule)
            df_clean = df_dedup.filter(f"NOT ({quarantine_rule})")
            
            bad_count = df_bad.count()
            if bad_count > 0:
                print(f"   [WARNING] Intercepted {bad_count} records violating rules. Routing to Quarantine.")
                df_bad.withColumn("quarantined_at", current_timestamp()) \
                      .write.format("parquet").mode("append").save(quarantine_target_path)

        # D. SILVER COMMIT
        df_silver = df_clean.withColumn("silver_timestamp", current_timestamp())
        df_silver.write.format("parquet").mode("overwrite").save(silver_target_path)
        
        rows_processed = df_silver.count()
        status = "Success"
        print(f"Success! {rows_processed} clean records advanced to Silver.")

    except Exception as e:
        error_message = str(e)
        print(f"Pipeline failure in Silver Layer: {error_message[:100]}")
        
    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        clean_err = error_message.replace("'", "''")
        
        # Ensure audit table exists here as well
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {audit_table} (
                table_name STRING, layer STRING, start_time TIMESTAMP, 
                end_time TIMESTAMP, duration_seconds DOUBLE, 
                rows_processed LONG, status STRING, error_message STRING
            )
        """)
        
        spark.sql(f"""
            INSERT INTO {audit_table} VALUES (
                '{target_name}', 'Silver_V2', '{start_time.strftime('%Y-%m-%d %H:%M:%S')}', 
                '{end_time.strftime('%Y-%m-%d %H:%M:%S')}', {duration}, 
                {rows_processed}, '{status}', '{clean_err}'
            )
        """)