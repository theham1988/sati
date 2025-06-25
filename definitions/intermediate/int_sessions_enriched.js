const clients = require("includes/clients.js");

for (const client of clients) {
  if (!client.source_dataset) continue;
  
  publish(`int_${client.name}_sessions_enriched`, {
    type: "table",
    schema: client.output_schema,
    tags: ["intermediate"],
    dependencies: [`stg_${client.name}_ga4_events`],
    bigquery: {
      partitionBy: "session_date",
      clusterBy: ["source", "medium", "campaign"]
    }
  }).query(ctx => `
    WITH session_events AS (
      SELECT
        user_pseudo_id,
        session_id,
        MIN(event_timestamp) AS session_start_timestamp,
        MAX(event_timestamp) AS session_end_timestamp,
        DATETIME(TIMESTAMP_MICROS(MIN(event_timestamp))) AS session_start_time,
        DATETIME(TIMESTAMP_MICROS(MAX(event_timestamp))) AS session_end_time,
        DATE(TIMESTAMP_MICROS(MIN(event_timestamp))) AS session_date,
        
        COUNT(DISTINCT event_name) AS unique_events,
        COUNT(*) AS total_events,
        
        MAX(CASE WHEN event_name = 'session_start' THEN source END) AS source,
        MAX(CASE WHEN event_name = 'session_start' THEN medium END) AS medium,
        MAX(CASE WHEN event_name = 'session_start' THEN campaign END) AS campaign,
        MAX(CASE WHEN event_name = 'session_start' THEN keyword END) AS keyword,
        MAX(CASE WHEN event_name = 'session_start' THEN ad_content END) AS ad_content,
        
        MAX(CASE WHEN event_name = 'session_start' THEN device.category END) AS device_category,
        MAX(CASE WHEN event_name = 'session_start' THEN device.os END) AS operating_system,
        MAX(CASE WHEN event_name = 'session_start' THEN device.browser END) AS browser,
        
        MAX(CASE WHEN event_name = 'session_start' THEN geo.country END) AS country,
        MAX(CASE WHEN event_name = 'session_start' THEN geo.region END) AS region,
        MAX(CASE WHEN event_name = 'session_start' THEN geo.city END) AS city,
        
        ARRAY_AGG(DISTINCT page.page_location IGNORE NULLS) AS pages_viewed,
        COUNT(DISTINCT page.page_location) AS unique_pages_viewed,
        
        MAX(engagement_time_msec) AS total_engagement_time_msec,
        
        MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS has_conversion,
        MAX(CASE WHEN event_name IN ('add_to_cart', 'begin_checkout') THEN 1 ELSE 0 END) AS has_engagement,
        
        SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.total_value ELSE 0 END) AS session_revenue,
        MAX(CASE WHEN event_name = 'purchase' THEN ecommerce.currency END) AS currency,
        
        STRING_AGG(DISTINCT click_ids.gclid IGNORE NULLS) AS gclids,
        
        MAX(session_number) AS session_number,
        
        MAX(_dataform_loaded_at) AS _dataform_loaded_at
        
      FROM ${ctx.ref(`stg_${client.name}_ga4_events`)}
      WHERE session_id IS NOT NULL
      GROUP BY user_pseudo_id, session_id
    ),
    
    session_metrics AS (
      SELECT
        *,
        TIMESTAMP_DIFF(session_end_timestamp, session_start_timestamp, SECOND) AS session_duration_seconds,
        
        CASE 
          WHEN total_events = 1 THEN 1
          ELSE 0
        END AS is_bounce,
        
        CASE
          WHEN LOWER(source) = 'google' AND LOWER(medium) = 'cpc' THEN 'Google Ads'
          WHEN LOWER(source) = 'facebook' AND LOWER(medium) IN ('cpc', 'paid') THEN 'Facebook Ads'
          WHEN LOWER(source) = 'instagram' AND LOWER(medium) IN ('cpc', 'paid') THEN 'Instagram Ads'
          WHEN LOWER(medium) = 'organic' THEN 'Organic Search'
          WHEN LOWER(medium) = 'social' THEN 'Organic Social'
          WHEN LOWER(medium) = 'email' THEN 'Email'
          WHEN source = '(direct)' AND medium = '(none)' THEN 'Direct'
          WHEN LOWER(medium) LIKE '%paid%' OR LOWER(medium) = 'cpc' THEN 'Other Paid'
          ELSE 'Other'
        END AS channel_grouping,
        
        CASE
          WHEN gclids IS NOT NULL THEN gclids
          WHEN LOWER(source) = 'google' AND LOWER(medium) = 'cpc' THEN CONCAT('inferred_', campaign, '_', session_date)
          ELSE NULL
        END AS attribution_id
        
      FROM session_events
    )
    
    SELECT * FROM session_metrics
  `);
}