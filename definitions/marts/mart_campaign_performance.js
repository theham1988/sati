const clients = require("includes/clients.js");

for (const client of clients) {
  const hasGA4 = client.source_dataset ? true : false;
  const hasGoogleAds = client.google_ads_dataset ? true : false;
  
  // Skip clients with no data sources
  if (!hasGA4 && !hasGoogleAds) continue;
  
  publish(`mart_${client.name}_campaign_performance`, {
    type: "table",
    schema: client.output_schema,
    tags: ["marts"],
    bigquery: {
      partitionBy: "date",
      clusterBy: ["channel_grouping", "campaign"]
    },
    assertions: {
      nonNull: ["date", "channel_grouping"],
      uniqueKey: ["date", "channel_grouping", "campaign", "source", "medium"]
    }
  }).query(ctx => {
    if (hasGA4 && hasGoogleAds) {
      // Full integration: GA4 + Google Ads
      return `
      WITH daily_sessions AS (
        SELECT
          session_date AS date,
          channel_grouping,
          source,
          medium,
          campaign,
          
          COUNT(DISTINCT user_pseudo_id) AS users,
          COUNT(DISTINCT session_id) AS sessions,
          SUM(CASE WHEN is_bounce = 1 THEN 1 ELSE 0 END) AS bounces,
          SUM(session_duration_seconds) / COUNT(*) AS avg_session_duration,
          SUM(unique_pages_viewed) / COUNT(*) AS avg_pages_per_session
          
        FROM ${ctx.ref(`int_${client.name}_sessions_enriched`)}
        GROUP BY 1, 2, 3, 4, 5
      ),
      
      daily_conversions AS (
        SELECT
          conversion_date AS date,
          channel_grouping,
          source,
          medium,
          campaign,
          
          COUNT(DISTINCT transaction_id) AS transactions,
          COUNT(DISTINCT user_pseudo_id) AS converting_users,
          SUM(revenue) AS revenue,
          COUNT(*) AS total_conversions,
          SUM(CASE WHEN conversion_category = 'Purchase' THEN 1 ELSE 0 END) AS purchases,
          SUM(CASE WHEN conversion_category = 'Lead' THEN 1 ELSE 0 END) AS leads,
          SUM(CASE WHEN customer_type = 'New Customer' THEN revenue ELSE 0 END) AS new_customer_revenue,
          AVG(minutes_to_conversion) AS avg_minutes_to_conversion
          
        FROM ${ctx.ref(`int_${client.name}_conversions_attributed`)}
        GROUP BY 1, 2, 3, 4, 5
      ),
      
      daily_costs AS (
        SELECT
          segments_date AS date,
          'Google Ads' AS channel_grouping,
          'google' AS source,
          'cpc' AS medium,
          campaign_name AS campaign,
          
          SUM(cost) AS cost,
          SUM(clicks) AS clicks,
          SUM(impressions) AS impressions
          
        FROM ${ctx.ref(`stg_${client.name}_google_ads_cost`)}
        GROUP BY 1, 2, 3, 4, 5
      ),
      
      combined_metrics AS (
        SELECT
          COALESCE(s.date, c.date, co.date) AS date,
          COALESCE(s.channel_grouping, c.channel_grouping, co.channel_grouping) AS channel_grouping,
          COALESCE(s.source, c.source, co.source) AS source,
          COALESCE(s.medium, c.medium, co.medium) AS medium,
          COALESCE(s.campaign, c.campaign, co.campaign) AS campaign,
          
          COALESCE(s.users, 0) AS users,
          COALESCE(s.sessions, 0) AS sessions,
          COALESCE(s.bounces, 0) AS bounces,
          COALESCE(s.avg_session_duration, 0) AS avg_session_duration,
          COALESCE(s.avg_pages_per_session, 0) AS avg_pages_per_session,
          
          COALESCE(c.transactions, 0) AS transactions,
          COALESCE(c.converting_users, 0) AS converting_users,
          COALESCE(c.revenue, 0) AS revenue,
          COALESCE(c.total_conversions, 0) AS total_conversions,
          COALESCE(c.purchases, 0) AS purchases,
          COALESCE(c.leads, 0) AS leads,
          COALESCE(c.new_customer_revenue, 0) AS new_customer_revenue,
          COALESCE(c.avg_minutes_to_conversion, 0) AS avg_minutes_to_conversion,
          
          COALESCE(co.cost, 0) AS cost,
          COALESCE(co.clicks, 0) AS clicks,
          COALESCE(co.impressions, 0) AS impressions
          
        FROM daily_sessions s
        FULL OUTER JOIN daily_conversions c
          ON s.date = c.date
          AND s.channel_grouping = c.channel_grouping
          AND s.source = c.source
          AND s.medium = c.medium
          AND s.campaign = c.campaign
        FULL OUTER JOIN daily_costs co
          ON COALESCE(s.date, c.date) = co.date
          AND COALESCE(s.channel_grouping, c.channel_grouping) = co.channel_grouping
          AND COALESCE(s.source, c.source) = co.source
          AND COALESCE(s.medium, c.medium) = co.medium
          AND COALESCE(s.campaign, c.campaign) = co.campaign
      )
      
      SELECT
        *,
        SAFE_DIVIDE(bounces, sessions) AS bounce_rate,
        SAFE_DIVIDE(transactions, sessions) AS conversion_rate,
        SAFE_DIVIDE(revenue, transactions) AS avg_order_value,
        SAFE_DIVIDE(revenue, cost) AS roas,
        SAFE_DIVIDE(cost, clicks) AS avg_cpc,
        SAFE_DIVIDE(clicks, impressions) AS ctr,
        SAFE_DIVIDE(cost, transactions) AS cost_per_acquisition,
        SAFE_DIVIDE(new_customer_revenue, revenue) AS new_customer_revenue_ratio,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM combined_metrics
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      `;
    } else if (hasGA4 && !hasGoogleAds) {
      // GA4 only (no Google Ads)
      return `
      WITH daily_sessions AS (
        SELECT
          session_date AS date,
          channel_grouping,
          source,
          medium,
          campaign,
          
          COUNT(DISTINCT user_pseudo_id) AS users,
          COUNT(DISTINCT session_id) AS sessions,
          SUM(CASE WHEN is_bounce = 1 THEN 1 ELSE 0 END) AS bounces,
          SUM(session_duration_seconds) / COUNT(*) AS avg_session_duration,
          SUM(unique_pages_viewed) / COUNT(*) AS avg_pages_per_session
          
        FROM ${ctx.ref(`int_${client.name}_sessions_enriched`)}
        GROUP BY 1, 2, 3, 4, 5
      ),
      
      daily_conversions AS (
        SELECT
          conversion_date AS date,
          channel_grouping,
          source,
          medium,
          campaign,
          
          COUNT(DISTINCT transaction_id) AS transactions,
          COUNT(DISTINCT user_pseudo_id) AS converting_users,
          SUM(revenue) AS revenue,
          COUNT(*) AS total_conversions,
          SUM(CASE WHEN conversion_category = 'Purchase' THEN 1 ELSE 0 END) AS purchases,
          SUM(CASE WHEN conversion_category = 'Lead' THEN 1 ELSE 0 END) AS leads,
          SUM(CASE WHEN customer_type = 'New Customer' THEN revenue ELSE 0 END) AS new_customer_revenue,
          AVG(minutes_to_conversion) AS avg_minutes_to_conversion
          
        FROM ${ctx.ref(`int_${client.name}_conversions_attributed`)}
        GROUP BY 1, 2, 3, 4, 5
      )
      
      SELECT
        COALESCE(s.date, c.date) AS date,
        COALESCE(s.channel_grouping, c.channel_grouping) AS channel_grouping,
        COALESCE(s.source, c.source) AS source,
        COALESCE(s.medium, c.medium) AS medium,
        COALESCE(s.campaign, c.campaign) AS campaign,
        
        COALESCE(s.users, 0) AS users,
        COALESCE(s.sessions, 0) AS sessions,
        COALESCE(s.bounces, 0) AS bounces,
        COALESCE(s.avg_session_duration, 0) AS avg_session_duration,
        COALESCE(s.avg_pages_per_session, 0) AS avg_pages_per_session,
        
        COALESCE(c.transactions, 0) AS transactions,
        COALESCE(c.converting_users, 0) AS converting_users,
        COALESCE(c.revenue, 0) AS revenue,
        COALESCE(c.total_conversions, 0) AS total_conversions,
        COALESCE(c.purchases, 0) AS purchases,
        COALESCE(c.leads, 0) AS leads,
        COALESCE(c.new_customer_revenue, 0) AS new_customer_revenue,
        COALESCE(c.avg_minutes_to_conversion, 0) AS avg_minutes_to_conversion,
        
        0 AS cost,
        0 AS clicks,
        0 AS impressions,
        
        SAFE_DIVIDE(bounces, sessions) AS bounce_rate,
        SAFE_DIVIDE(transactions, sessions) AS conversion_rate,
        SAFE_DIVIDE(revenue, transactions) AS avg_order_value,
        NULL AS roas,
        NULL AS avg_cpc,
        NULL AS ctr,
        NULL AS cost_per_acquisition,
        SAFE_DIVIDE(new_customer_revenue, revenue) AS new_customer_revenue_ratio,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM daily_sessions s
      FULL OUTER JOIN daily_conversions c
        ON s.date = c.date
        AND s.channel_grouping = c.channel_grouping
        AND s.source = c.source
        AND s.medium = c.medium
        AND s.campaign = c.campaign
      WHERE COALESCE(s.date, c.date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      `;
    } else if (!hasGA4 && hasGoogleAds) {
      // Google Ads only (no GA4/website data)
      return `
      WITH daily_costs AS (
        SELECT
          segments_date AS date,
          'Google Ads' AS channel_grouping,
          'google' AS source,
          'cpc' AS medium,
          campaign_name AS campaign,
          
          SUM(cost) AS cost,
          SUM(clicks) AS clicks,
          SUM(impressions) AS impressions,
          SUM(conversions) AS transactions,
          SUM(conversions_value) AS revenue
          
        FROM ${ctx.ref(`stg_${client.name}_google_ads_cost`)}
        GROUP BY 1, 2, 3, 4, 5
      )
      
      SELECT
        date,
        channel_grouping,
        source,
        medium,
        campaign,
        
        0 AS users,
        0 AS sessions,
        0 AS bounces,
        0 AS avg_session_duration,
        0 AS avg_pages_per_session,
        
        transactions,
        0 AS converting_users,
        revenue,
        transactions AS total_conversions,
        0 AS purchases,
        0 AS leads,
        0 AS new_customer_revenue,
        0 AS avg_minutes_to_conversion,
        
        cost,
        clicks,
        impressions,
        
        0 AS bounce_rate,
        SAFE_DIVIDE(transactions, clicks) AS conversion_rate,
        SAFE_DIVIDE(revenue, transactions) AS avg_order_value,
        SAFE_DIVIDE(revenue, cost) AS roas,
        SAFE_DIVIDE(cost, clicks) AS avg_cpc,
        SAFE_DIVIDE(clicks, impressions) AS ctr,
        SAFE_DIVIDE(cost, transactions) AS cost_per_acquisition,
        0 AS new_customer_revenue_ratio,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM daily_costs
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      `;
    }
  });
}