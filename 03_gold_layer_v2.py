from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import current_timestamp, lit, col, concat_ws, sha2, row_number, max, lpad, coalesce
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# ====================================================================
# 1. THE DELTA CONTROL ENGINE & CONFIGURATION (V2)
# ====================================================================
print("V2 GOLD WAREHOUSE: DELTA CONTROL TABLE MODE\n")

silver_zone = "/Volumes/workspace/default/silver_business_v2/"
gold_zone = "/Volumes/workspace/default/gold_business_v2/"
audit_table = "workspace.default.pipeline_audit_logs_v2"

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# Fetch the global execution parameters directly from the Delta table
df_exec = spark.read.table("workspace.default.pipeline_execution_config_v2").limit(1)
exec_cfg = df_exec.collect()[0].asDict() if df_exec.count() > 0 else {}

# Fetch routing rules
df_config = spark.read.table("workspace.default.pipeline_configuration_v2") \
                 .filter("is_active = true") \
                 .orderBy("process_priority")

tables_config = [row.asDict(recursive=True) for row in df_config.collect()]

# --- THE FIX: Smart Audit-Based Cascading Skip ---
try:
    # Query the audit table to see what Silver just did
    df_last_silver = spark.sql(f"""
        SELECT status 
        FROM {audit_table} 
        WHERE layer = 'Silver_V2' 
        ORDER BY end_time DESC 
        LIMIT 1
    """)
    
    if df_last_silver.count() > 0:
        last_status = df_last_silver.collect()[0]["status"]
        
        if last_status == 'SKIPPED_NO_FILES':
            print("Detected Silver layer skipped (No new files). Gold standing by.")
            
            start_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            spark.sql(f"""
                INSERT INTO {audit_table} VALUES (
                    'ALL_TARGETS', 'Gold_V2', '{start_time_str}', 
                    '{start_time_str}', 0.0, 
                    0, 'SKIPPED_NO_FILES', 'None'
                )
            """)
            
            # Exit immediately, saving compute
            dbutils.notebook.exit("SKIP_PIPELINE")

except Exception as e:
    print("Audit table check failed or table missing. Proceeding with run.")

# Set up active tables for processing if we didn't exit
active_tables_with_data = tables_config

# ====================================================================
# 2. V2 GOLD PROCESSING ENGINE
# ====================================================================
# We now loop over active_tables_with_data instead of tables_config
for rules in active_tables_with_data:
    target_name = rules.get('target_table')
    silver_path = f"{silver_zone}{target_name}/"
    gold_path = f"{gold_zone}{target_name}/"
    
    print(f"\n--- Constructing Gold Table: {target_name} ---")
    
    strategy = rules.get('scd_strategy', 'SCD1')
    natural_keys = rules.get('natural_keys', [])
    sk_name = target_name.replace("gold_", "").replace("_dim", "").replace("_fact", "") + "_sk"
    
    start_time = datetime.now()
    status, error_message, rows_processed = "Failed", "None", 0
    
    try:
        # A. LOAD & SNAPSHOT CONTROL (Driven by the database)
        df_silver = spark.read.parquet(silver_path)

        if exec_cfg.get('is_snapshot') and exec_cfg.get('snapshot_layer', '').lower() in ['gold', 'all']:
            if exec_cfg.get('sample_type') == "limit":
                df_silver = df_silver.limit(exec_cfg.get('row_limit', 100))
            else:
                df_silver = df_silver.sample(withReplacement=False, fraction=exec_cfg.get('sample_fraction', 0.1))

        # B. HASH GENERATION & TIMESTAMPS
        all_cols = df_silver.columns
        business_cols = sorted([c for c in all_cols if c not in natural_keys and c not in ["silver_timestamp", "ingest_timestamp", "source_file"]])

        df_staging = df_silver.withColumn(
            "hash_key", 
            sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in business_cols]), 256)
        )

        df_staging = df_staging.withColumn("effective_start_date", current_timestamp()) \
                               .withColumn("effective_end_date", lit("9999-12-31 00:00:00").cast("timestamp")) \
                               .withColumn("is_active", lit(True))

        # ====================================================================
        # C. SURROGATE KEY (SK) PRESERVATION & ASSIGNMENT
        # ====================================================================
        table_exists = path_exists(gold_path) and DeltaTable.isDeltaTable(spark, gold_path)
        base_id = 0
        
        if table_exists:
            df_existing_gold = spark.read.format("delta").load(gold_path)
            max_id_val = df_existing_gold.select(max(col(sk_name).cast(IntegerType()))).collect()[0][0]
            if max_id_val is not None:
                base_id = int(max_id_val)
                
        # If SCD1, look up existing SKs to prevent overwriting them
        if table_exists and strategy == 'SCD1':
            df_existing_sks = df_existing_gold.select(*natural_keys, col(sk_name).alias("existing_sk")).distinct()
            df_staging = df_staging.join(df_existing_sks, on=natural_keys, how="left")
        else:
            df_staging = df_staging.withColumn("existing_sk", lit(None).cast("string"))

        # Generate a continuous sequence ONLY for the new records
        window_spec = Window.partitionBy(col("existing_sk").isNull()).orderBy(*[col(k) for k in natural_keys])
        
        df_staging = df_staging.withColumn(
            "new_sk", 
            lpad((lit(base_id) + row_number().over(window_spec)).cast("string"), 3, "0")
        )

        # Merge them: Keep old SK if it exists, otherwise assign the new one
        df_staging = df_staging.withColumn(
            sk_name, 
            coalesce(col("existing_sk"), col("new_sk"))
        ).drop("existing_sk", "new_sk")
        # ====================================================================

        # D. SCHEMA ALIGNMENT
        metadata_cols = [c for c in all_cols if c in ["ingest_timestamp", "source_file", "silver_timestamp"]]
        ordered_columns = ([sk_name] + natural_keys + business_cols + metadata_cols + 
                           ["effective_start_date", "effective_end_date", "is_active", "hash_key"])
        
        df_final_gold = df_staging.select(*[col(c) for c in ordered_columns if c in df_staging.columns])

        # E. DELTA MERGE (SCD ENGINE)
        join_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in natural_keys])

        if not table_exists:
            df_final_gold.write.format("delta").mode("overwrite").save(gold_path)
        else:
            delta_gold = DeltaTable.forPath(spark, gold_path)
            
            if strategy == 'SCD1':
                delta_gold.alias("tgt").merge(
                    df_final_gold.alias("src"), condition=join_cond
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
            elif strategy == 'SCD2':
                delta_gold.alias("tgt").merge(
                    df_final_gold.alias("src"), 
                    condition=f"{join_cond} AND tgt.is_active = True AND tgt.hash_key != src.hash_key"
                ).whenMatchedUpdate(set={
                    "is_active": lit(False),
                    "effective_end_date": current_timestamp()
                }).execute()
                
                df_active_gold = spark.read.format("delta").load(gold_path).filter(col("is_active") == True)
                anti_join_cond = [col(f"src.{k}") == col(f"tgt.{k}") for k in natural_keys] + [col("src.hash_key") == col("tgt.hash_key")]
                
                df_new_records = df_final_gold.alias("src").join(
                    df_active_gold.alias("tgt"),
                    on=anti_join_cond,
                    how="left_anti"
                )
                
                if df_new_records.count() > 0:
                    df_new_records.write.format("delta").mode("append").save(gold_path)
            
        rows_processed = df_final_gold.count()
        status = "Success"
        print(f"Success! Processed {rows_processed} records into Gold using {strategy}.")

        # F. ISOLATED BATCH VALIDATION VIEW
        print(f"\n============================================================")
        print(f"METADATA CONTROL RULES APPLIED: {target_name}")
        print(f"============================================================")
        display(df_config.filter(col("target_table") == target_name))

        print("\nWarehouse Validation View (Showing Max 7 Rows for UI Stability):")
        df_gold_output = spark.read.format("delta").load(gold_path)
        df_batch_keys = df_final_gold.select(natural_keys).distinct()
        
        display(df_gold_output.join(df_batch_keys, on=natural_keys, how="inner").limit(7))

    except Exception as e:
        error_message = str(e)
        print(f"Pipeline failure in Gold layer execution: {error_message[:100]}\n")
        
    finally:
        # G. PIPELINE AUDIT LOGGING
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
                '{target_name}', 'Gold_V2', '{start_time.strftime('%Y-%m-%d %H:%M:%S')}', 
                '{end_time.strftime('%Y-%m-%d %H:%M:%S')}', {duration}, 
                {rows_processed}, '{status}', '{clean_err}'
            )
        """)