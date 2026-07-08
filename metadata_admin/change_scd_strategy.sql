UPDATE workspace.default.pipeline_configuration_v2
SET scd_strategy = 'SCD2' -- (Change this to 'SCD1' if that is your naming convention)
WHERE target_table = 'gold_employee_dim';