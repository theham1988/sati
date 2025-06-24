const clients = require("includes/clients.js");

for (const client of clients) {
  if (!client.google_ads_dataset || !client.google_ads_customer_id) continue;
  
  publish(`stg_${client.name}_google_ads_cost`, {
    type: "table",
    schema: client.output_schema,
    tags: ["staging"],
    bigquery: {
      partitionBy: "date"
    }
  }).query(() => `
    SELECT
      -- Core fields for ROAS tracking
      segments_date AS date,
      campaign_id,
      campaign_name,
      
      -- Cost and performance metrics
      metrics_cost_micros / 1000000.0 AS cost,
      metrics_clicks AS clicks,
      metrics_impressions AS impressions,
      metrics_conversions AS conversions,
      metrics_conversions_value AS conversions_value
      
    FROM \`${client.project_id}.${client.google_ads_dataset}.p_ads_CampaignBasicStats_${client.google_ads_customer_id}\`
    WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  `);
}