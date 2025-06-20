const clients = require("includes/clients.js");

for (const client of clients) {
  // Skip clients without GA4 data
  if (!client.source_dataset) continue;
  
  publish(`${client.name}_user`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      user_pseudo_id,
      user_id,
      
      -- First visit information
      MIN(IF(event_name = 'first_visit', event_timestamp, NULL)) AS first_visit_timestamp,
      MIN(IF(event_name = 'first_visit', event_date, NULL)) AS first_visit_date,
      
      -- Last activity
      MAX(event_timestamp) AS last_activity_timestamp,
      MAX(event_date) AS last_activity_date,
      
      -- Engagement metrics
      COUNT(DISTINCT event_name) AS unique_events,
      COUNT(*) AS total_events,
      COUNT(DISTINCT event_date) AS active_days,
      COUNT(DISTINCT session_id) AS total_sessions,
      
      -- Conversion metrics
      COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS total_purchases,
      COUNT(CASE WHEN event_name = 'generate_lead' THEN 1 END) AS total_leads,
      SUM(CASE WHEN event_name = 'purchase' 
          THEN COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value"), 0)
          ELSE 0 END) AS total_revenue,
      
      -- Device and location preferences
      ARRAY_AGG(DISTINCT device.category IGNORE NULLS)[OFFSET(0)] AS primary_device,
      ARRAY_AGG(DISTINCT geo.country IGNORE NULLS)[OFFSET(0)] AS primary_country,
      ARRAY_AGG(DISTINCT geo.region IGNORE NULLS)[OFFSET(0)] AS primary_region,
      ARRAY_AGG(DISTINCT geo.city IGNORE NULLS)[OFFSET(0)] AS primary_city,
      
      -- Traffic source preferences
      ARRAY_AGG(DISTINCT traffic_source.source IGNORE NULLS)[OFFSET(0)] AS primary_source,
      ARRAY_AGG(DISTINCT traffic_source.medium IGNORE NULLS)[OFFSET(0)] AS primary_medium,
      ARRAY_AGG(DISTINCT traffic_source.name IGNORE NULLS)[OFFSET(0)] AS primary_campaign,
      
      -- User properties (if available)
      ARRAY_AGG(DISTINCT user_properties.value.string_value IGNORE NULLS)[OFFSET(0)] AS user_property_1,
      
      -- Calculated fields
      CASE 
        WHEN COUNT(DISTINCT event_date) = 1 THEN 'single_day'
        WHEN COUNT(DISTINCT event_date) <= 7 THEN 'weekly'
        WHEN COUNT(DISTINCT event_date) <= 30 THEN 'monthly'
        ELSE 'regular'
      END AS user_frequency,
      
      CASE 
        WHEN COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) > 0 THEN 'converted'
        WHEN COUNT(CASE WHEN event_name = 'generate_lead' THEN 1 END) > 0 THEN 'lead'
        ELSE 'visitor'
      END AS user_type,
      
      -- Recency calculation
      DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) AS days_since_last_activity,
      
      -- Lifetime value calculation
      CASE 
        WHEN COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) > 0 
        THEN SUM(CASE WHEN event_name = 'purchase' 
            THEN COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value"), 0)
            ELSE 0 END)
        ELSE 0 
      END AS lifetime_value
      
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
    GROUP BY user_pseudo_id, user_id
  `);
}
