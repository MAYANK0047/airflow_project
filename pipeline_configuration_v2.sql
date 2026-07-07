CREATE TABLE workspace.default.pipeline_configuration_v2 (
  target_table STRING COLLATE UTF8_BINARY,
  source_type STRING COLLATE UTF8_BINARY,
  file_pattern STRING COLLATE UTF8_BINARY,
  row_tag STRING COLLATE UTF8_BINARY,
  scd_strategy STRING COLLATE UTF8_BINARY,
  natural_keys ARRAY<STRING COLLATE UTF8_BINARY>,
  quarantine_rules STRING COLLATE UTF8_BINARY,
  process_priority INT,
  is_active BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')