-- Check tables in gofresh_ct dataset
SELECT 
  table_catalog,
  table_schema as dataset_name,
  table_name,
  table_type,
  creation_time
FROM `sublime-elixir-458201-r4.gofresh_ct.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;

-- Check tables in gofresh_fuel dataset
SELECT 
  table_catalog,
  table_schema as dataset_name,
  table_name,
  table_type,
  creation_time
FROM `sublime-elixir-458201-r4.gofresh_fuel.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;