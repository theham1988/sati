const clients = require("includes/clients.js");

for (const client of clients) {
  // Skip clients without GA4 data
  if (!client.source_dataset) continue;
  
  publish(`${client.name}_daily_metrics`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    WITH daily_events AS (
      SELECT
        event_date,
        COUNT(*) AS total_events,
        COUNT(DISTINCT user_pseudo_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions,
        COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN session_id END) AS sessions_started,
        COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases,
        COUNT(CASE WHEN event_name = 'generate_lead' THEN 1 END) AS leads,
        COUNT(CASE WHEN event_name = 'form_submit' THEN 1 END) AS form_submissions,
        COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS page_views,
        COUNT(CASE WHEN event_name = 'scroll' THEN 1 END) AS scrolls,
        COUNT(CASE WHEN event_name = 'click' THEN 1 END) AS clicks,
        
        -- Revenue metrics
        SUM(CASE WHEN event_name = 'purchase' 
            THEN COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value"), 0)
            ELSE 0 END) AS total_revenue,
            
        -- Engagement metrics
        AVG(CASE WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "engagement_time_msec") > 0
            THEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "engagement_time_msec")
            ELSE NULL END) AS avg_engagement_time_msec,
            
        -- Traffic source breakdown
        COUNT(CASE WHEN traffic_source.source = 'google' THEN 1 END) AS google_events,
        COUNT(CASE WHEN traffic_source.source = 'facebook' THEN 1 END) AS facebook_events,
        COUNT(CASE WHEN traffic_source.source = 'direct' THEN 1 END) AS direct_events,
        COUNT(CASE WHEN traffic_source.source = 'organic' THEN 1 END) AS organic_events,
        
        -- Device breakdown
        COUNT(CASE WHEN device.category = 'desktop' THEN 1 END) AS desktop_events,
        COUNT(CASE WHEN device.category = 'mobile' THEN 1 END) AS mobile_events,
        COUNT(CASE WHEN device.category = 'tablet' THEN 1 END) AS tablet_events,
        
        -- Geographic breakdown
        COUNT(CASE WHEN geo.country = 'Thailand' THEN 1 END) AS thailand_events,
        COUNT(CASE WHEN geo.country != 'Thailand' THEN 1 END) AS international_events
        
      FROM \`${client.project_id}.${client.source_dataset}.events_*\`
      WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
      GROUP BY event_date
    )
    
    SELECT
      event_date,
      total_events,
      unique_users,
      unique_sessions,
      sessions_started,
      purchases,
      leads,
      form_submissions,
      page_views,
      scrolls,
      clicks,
      total_revenue,
      avg_engagement_time_msec,
      
      -- Calculated metrics
      CASE WHEN unique_sessions > 0 THEN ROUND(unique_users / unique_sessions, 2) ELSE 0 END AS users_per_session,
      CASE WHEN unique_users > 0 THEN ROUND(total_events / unique_users, 2) ELSE 0 END AS events_per_user,
      CASE WHEN page_views > 0 THEN ROUND(CAST(purchases AS FLOAT64) / page_views * 100, 4) ELSE 0 END AS conversion_rate,
      CASE WHEN sessions_started > 0 THEN ROUND(CAST(purchases AS FLOAT64) / sessions_started * 100, 4) ELSE 0 END AS purchase_rate,
      
      -- Traffic source percentages
      CASE WHEN total_events > 0 THEN ROUND(google_events / total_events * 100, 2) ELSE 0 END AS google_percentage,
      CASE WHEN total_events > 0 THEN ROUND(facebook_events / total_events * 100, 2) ELSE 0 END AS facebook_percentage,
      CASE WHEN total_events > 0 THEN ROUND(direct_events / total_events * 100, 2) ELSE 0 END AS direct_percentage,
      CASE WHEN total_events > 0 THEN ROUND(organic_events / total_events * 100, 2) ELSE 0 END AS organic_percentage,
      
      -- Device percentages
      CASE WHEN total_events > 0 THEN ROUND(desktop_events / total_events * 100, 2) ELSE 0 END AS desktop_percentage,
      CASE WHEN total_events > 0 THEN ROUND(mobile_events / total_events * 100, 2) ELSE 0 END AS mobile_percentage,
      CASE WHEN total_events > 0 THEN ROUND(tablet_events / total_events * 100, 2) ELSE 0 END AS tablet_percentage,
      
      -- Geographic percentages
      CASE WHEN total_events > 0 THEN ROUND(thailand_events / total_events * 100, 2) ELSE 0 END AS thailand_percentage,
      CASE WHEN total_events > 0 THEN ROUND(international_events / total_events * 100, 2) ELSE 0 END AS international_percentage
      
    FROM daily_events
    ORDER BY event_date DESC
  `);
} 