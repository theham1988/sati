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
        
        MAX(CASE WHEN event_name = 'session_start' THEN device_category END) AS device_category,
        
        MAX(CASE WHEN event_name = 'session_start' THEN country END) AS country,
        
        -- Dynamic conversions based on client configuration
        ${client.conversion_events ? `MAX(CASE WHEN event_name IN (${client.conversion_events.map(e => `'${e}'`).join(', ')}) THEN 1 ELSE 0 END)` : '0'} AS has_conversion,
        ${client.engagement_events ? `MAX(CASE WHEN event_name IN (${client.engagement_events.map(e => `'${e}'`).join(', ')}) THEN 1 ELSE 0 END)` : '0'} AS has_engagement,
        
        ${client.conversion_events ? `SUM(CASE WHEN event_name IN (${client.conversion_events.map(e => `'${e}'`).join(', ')}) THEN revenue ELSE 0 END)` : '0'} AS session_revenue,
        ${client.conversion_events ? `MAX(CASE WHEN event_name IN (${client.conversion_events.map(e => `'${e}'`).join(', ')}) THEN currency END)` : 'NULL'} AS currency,
        
        STRING_AGG(DISTINCT gclid) AS gclids,
        
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