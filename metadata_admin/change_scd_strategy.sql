UPDATE workspace.default.pipeline_configuration_v2
SET scd_strategy = 'SCD2' 
WHERE target_table = 'gold_employee_dim';