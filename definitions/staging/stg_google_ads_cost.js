const clients = require("includes/clients.js");

for (const client of clients) {
  if (!client.google_ads_dataset) continue;
  
  publish(`stg_${client.name}_google_ads_cost`, {
    type: "incremental",
    uniqueKey: ["segments_date", "campaign_id", "ad_group_id"],
    schema: client.output_schema,
    tags: ["staging"],
    bigquery: {
      partitionBy: "segments_date",
      clusterBy: ["campaign_id", "ad_group_id"]
    }
  }).query(ctx => `
    WITH ads_performance AS (
      SELECT
        segments.date AS segments_date,
        customer.id AS customer_id,
        customer.descriptive_name AS customer_name,
        
        campaign.id AS campaign_id,
        campaign.name AS campaign_name,
        campaign.status AS campaign_status,
        campaign.advertising_channel_type AS campaign_type,
        campaign.bidding_strategy_type,
        
        ad_group.id AS ad_group_id,
        ad_group.name AS ad_group_name,
        ad_group.status AS ad_group_status,
        
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros / 1000000.0 AS cost,
        metrics.conversions,
        metrics.conversions_value,
        metrics.view_through_conversions,
        
        metrics.average_cpc AS avg_cpc,
        metrics.average_cpm AS avg_cpm,
        metrics.ctr,
        metrics.cost_per_conversion,
        
        segments.device,
        segments.ad_network_type,
        segments.click_type,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM \`${client.project_id}.${client.google_ads_dataset}.ad_group_performance_report\`
      WHERE segments.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ${ctx.when(ctx.incremental(), `AND segments.date > (SELECT MAX(segments_date) FROM ${ctx.self()})`)}
    )
    
    SELECT * FROM ads_performance
  `);
}