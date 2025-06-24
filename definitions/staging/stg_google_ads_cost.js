const clients = require("includes/clients.js");

for (const client of clients) {
  // Only process clients that have Google Ads data configured
  if (!client.google_ads_dataset || !client.google_ads_customer_id) continue;
  
  publish(`stg_${client.name}_google_ads_cost`, {
    type: "table",
    schema: client.output_schema,
    tags: ["staging"],
    bigquery: {
      partitionBy: "segments_date",
      clusterBy: ["campaign_id"]
    }
  }).query(() => `
    WITH ads_performance AS (
      SELECT
        segments_date,
        customer_id,
        customer_descriptive_name AS customer_name,
        
        campaign_id,
        campaign_name,
        campaign_status,
        campaign_advertising_channel_type AS campaign_type,
        campaign_bidding_strategy_type AS bidding_strategy_type,
        
        CAST(NULL AS INT64) AS ad_group_id,
        CAST(NULL AS STRING) AS ad_group_name,
        CAST(NULL AS STRING) AS ad_group_status,
        
        metrics_clicks AS clicks,
        metrics_impressions AS impressions,
        metrics_cost_micros / 1000000.0 AS cost,
        metrics_conversions AS conversions,
        metrics_conversions_value AS conversions_value,
        metrics_view_through_conversions AS view_through_conversions,
        
        SAFE_DIVIDE(metrics_cost_micros / 1000000.0, metrics_clicks) AS avg_cpc,
        SAFE_DIVIDE(metrics_cost_micros / 1000000.0, metrics_impressions * 1000) AS avg_cpm,
        SAFE_DIVIDE(metrics_clicks, metrics_impressions) AS ctr,
        SAFE_DIVIDE(metrics_cost_micros / 1000000.0, metrics_conversions) AS cost_per_conversion,
        
        segments_device AS device,
        segments_ad_network_type AS ad_network_type,
        CAST(NULL AS STRING) AS click_type,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM \`${client.project_id}.${client.google_ads_dataset}.p_ads_CampaignBasicStats_${client.google_ads_customer_id}\`
      WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    )
    
    SELECT * FROM ads_performance
  `);
}