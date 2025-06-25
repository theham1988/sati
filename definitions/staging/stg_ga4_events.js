const clients = require("includes/clients.js");

for (const client of clients) {
  if (!client.source_dataset) continue;
  
  publish(`stg_${client.name}_ga4_events`, {
    type: "table",
    schema: client.output_schema,
    tags: ["staging"],
    bigquery: {
      partitionBy: "DATE(PARSE_DATE('%Y%m%d', event_date))",
      clusterBy: ["event_name", "user_pseudo_id"]
    }
  }).query(() => `
    SELECT
      -- Core fields for ROAS tracking
      event_timestamp,
      event_date,
      event_name,
      user_pseudo_id,
      
      -- Session tracking
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") AS session_id,
      
      -- Traffic source for attribution
      traffic_source.source,
      traffic_source.medium,
      traffic_source.name AS campaign,
      
      -- Revenue tracking
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = "value") AS revenue,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "currency") AS currency,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "transaction_id") AS transaction_id,
      
      -- Attribution
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "gclid") AS gclid,
      
      -- Location for reporting
      geo.country,
      device.category AS device_category
      
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  `);
}