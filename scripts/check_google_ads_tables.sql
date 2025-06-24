-- Script to discover Google Ads tables in BigQuery datasets
-- Run these queries in BigQuery Console to see what tables actually exist

-- 1. Check all tables in gofresh_fuel dataset
SELECT 
  table_name,
  table_type,
  creation_time,
  CASE 
    WHEN table_name LIKE 'p_ads_%' THEN 'Partitioned Google Ads Table'
    WHEN table_name LIKE 'ads_%' THEN 'Google Ads View'
    ELSE 'Other'
  END AS table_category
FROM `sublime-elixir-458201-r4.gofresh_fuel.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;

-- 2. Check all tables in gofresh_ct dataset
SELECT 
  table_name,
  table_type,
  creation_time,
  CASE 
    WHEN table_name LIKE 'p_ads_%' THEN 'Partitioned Google Ads Table'
    WHEN table_name LIKE 'ads_%' THEN 'Google Ads View'
    ELSE 'Other'
  END AS table_category
FROM `sublime-elixir-458201-r4.gofresh_ct.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;

-- 3. Check schema of a specific table (update table_name after finding it)
-- Example: If you find a table like 'p_ads_CampaignBasicStats_8545745456'
/*
SELECT 
  column_name,
  data_type,
  is_nullable,
  is_partitioning_column,
  clustering_ordinal_position
FROM `sublime-elixir-458201-r4.gofresh_ct.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'YOUR_TABLE_NAME_HERE'
ORDER BY ordinal_position;
*/

-- 4. Sample data from a table to understand structure (update table reference)
/*
SELECT *
FROM `sublime-elixir-458201-r4.gofresh_ct.YOUR_TABLE_NAME_HERE`
LIMIT 10;
*/

-- 5. Check if there are any wildcard tables
SELECT 
  table_name
FROM `sublime-elixir-458201-r4.gofresh_ct.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '%campaign%' 
   OR table_name LIKE '%ad_group%'
   OR table_name LIKE '%ads%'
ORDER BY table_name;

-- 6. Same check for gofresh_fuel
SELECT 
  table_name
FROM `sublime-elixir-458201-r4.gofresh_fuel.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '%campaign%' 
   OR table_name LIKE '%ad_group%'
   OR table_name LIKE '%ads%'
ORDER BY table_name;