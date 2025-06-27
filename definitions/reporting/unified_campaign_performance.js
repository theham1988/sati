const clients = require("includes/clients.js");

// Create unified campaign performance table combining all clients
publish("unified_campaign_performance", {
  type: "table",
  schema: "reporting",
  tags: ["reporting", "unified"],
  bigquery: {
    partitionBy: "date",
    clusterBy: ["business_type", "client_name", "channel_grouping"]
  },
  description: "Unified campaign performance across all clients for Looker Studio reporting"
}).query(ctx => {
  // Generate UNION ALL query for all clients
  const clientQueries = clients.map(client => {
    const hasGA4 = client.source_dataset ? true : false;
    const hasGoogleAds = client.google_ads_dataset ? true : false;
    
    // Skip clients with no data sources
    if (!hasGA4 && !hasGoogleAds) return null;
    
    return `
      SELECT
        '${client.name}' AS client_name,
        '${client.business_type}' AS business_type,
        date,
        channel_grouping,
        source,
        medium,
        campaign,
        
        -- Traffic Metrics
        users,
        sessions,
        bounces,
        avg_session_duration,
        bounce_rate,
        
        -- Conversion Metrics
        transactions,
        converting_users,
        revenue,
        total_conversions,
        purchases,
        leads,
        conversion_rate,
        avg_order_value,
        
        -- Revenue Metrics
        new_customer_revenue,
        new_customer_revenue_ratio,
        avg_minutes_to_conversion,
        
        -- Cost Metrics
        cost,
        clicks,
        impressions,
        avg_cpc,
        ctr,
        
        -- ROI Metrics
        roas,
        cost_per_acquisition,
        
        -- Metadata
        _dataform_loaded_at,
        CURRENT_TIMESTAMP() AS _unified_loaded_at
        
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
    `;
  }).filter(q => q !== null);
  
  return clientQueries.join('\nUNION ALL\n');
});

// Create business type summary views
const businessTypes = ['hotel', 'beach_club', 'restaurant'];

businessTypes.forEach(businessType => {
  publish(`reporting_${businessType}_summary`, {
    type: "view",
    schema: "reporting",
    tags: ["reporting", "summary", businessType],
    description: `Summary metrics for ${businessType} businesses`
  }).query(ctx => `
    WITH daily_metrics AS (
      SELECT
        date,
        client_name,
        
        -- Aggregate all metrics
        SUM(users) AS total_users,
        SUM(sessions) AS total_sessions,
        SUM(revenue) AS total_revenue,
        SUM(transactions) AS total_transactions,
        SUM(cost) AS total_cost,
        SUM(clicks) AS total_clicks,
        SUM(impressions) AS total_impressions,
        
        -- Calculate weighted averages
        SAFE_DIVIDE(SUM(bounces), SUM(sessions)) AS avg_bounce_rate,
        SAFE_DIVIDE(SUM(transactions), SUM(sessions)) AS avg_conversion_rate,
        SAFE_DIVIDE(SUM(revenue), SUM(transactions)) AS avg_order_value,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS overall_roas,
        SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS avg_cpc,
        SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS avg_ctr
        
      FROM ${ctx.ref("unified_campaign_performance")}
      WHERE business_type = '${businessType}'
      GROUP BY 1, 2
    ),
    
    last_30_days AS (
      SELECT
        client_name,
        SUM(total_revenue) AS revenue_30d,
        SUM(total_cost) AS cost_30d,
        SUM(total_transactions) AS transactions_30d,
        AVG(avg_conversion_rate) AS conversion_rate_30d,
        SAFE_DIVIDE(SUM(total_revenue), SUM(total_cost)) AS roas_30d
      FROM daily_metrics
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1
    ),
    
    prev_30_days AS (
      SELECT
        client_name,
        SUM(total_revenue) AS revenue_prev_30d,
        SUM(total_cost) AS cost_prev_30d,
        SUM(total_transactions) AS transactions_prev_30d,
        AVG(avg_conversion_rate) AS conversion_rate_prev_30d,
        SAFE_DIVIDE(SUM(total_revenue), SUM(total_cost)) AS roas_prev_30d
      FROM daily_metrics
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY 1
    )
    
    SELECT
      l.client_name,
      '${businessType}' AS business_type,
      
      -- Current period metrics
      l.revenue_30d,
      l.cost_30d,
      l.transactions_30d,
      l.conversion_rate_30d,
      l.roas_30d,
      
      -- Previous period metrics
      p.revenue_prev_30d,
      p.cost_prev_30d,
      p.transactions_prev_30d,
      p.conversion_rate_prev_30d,
      p.roas_prev_30d,
      
      -- Period-over-period changes
      SAFE_DIVIDE(l.revenue_30d - p.revenue_prev_30d, p.revenue_prev_30d) AS revenue_change,
      SAFE_DIVIDE(l.transactions_30d - p.transactions_prev_30d, p.transactions_prev_30d) AS transactions_change,
      l.conversion_rate_30d - p.conversion_rate_prev_30d AS conversion_rate_change,
      l.roas_30d - p.roas_prev_30d AS roas_change
      
    FROM last_30_days l
    LEFT JOIN prev_30_days p USING (client_name)
    ORDER BY l.revenue_30d DESC
  `);
});

// Create overall executive dashboard
publish("reporting_executive_dashboard", {
  type: "view", 
  schema: "reporting",
  tags: ["reporting", "executive"],
  description: "Executive dashboard with all clients grouped by business type"
}).query(ctx => `
  WITH business_type_metrics AS (
    SELECT
      business_type,
      date,
      
      -- Aggregate metrics by business type
      SUM(users) AS total_users,
      SUM(sessions) AS total_sessions,
      SUM(revenue) AS total_revenue,
      SUM(transactions) AS total_transactions,
      SUM(cost) AS total_cost,
      
      -- Calculate ratios
      SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS roas,
      SAFE_DIVIDE(SUM(transactions), SUM(sessions)) AS conversion_rate,
      SAFE_DIVIDE(SUM(revenue), SUM(transactions)) AS avg_order_value,
      
      COUNT(DISTINCT client_name) AS active_clients
      
    FROM ${ctx.ref("unified_campaign_performance")}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY 1, 2
  ),
  
  last_30_days_summary AS (
    SELECT
      business_type,
      SUM(total_revenue) AS revenue_30d,
      SUM(total_cost) AS cost_30d,
      SUM(total_transactions) AS transactions_30d,
      AVG(conversion_rate) AS avg_conversion_rate_30d,
      AVG(roas) AS avg_roas_30d,
      AVG(active_clients) AS avg_active_clients
    FROM business_type_metrics
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 1
  ),
  
  channel_performance AS (
    SELECT
      business_type,
      channel_grouping,
      SUM(revenue) AS channel_revenue,
      SUM(cost) AS channel_cost,
      SUM(transactions) AS channel_transactions,
      SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS channel_roas
    FROM ${ctx.ref("unified_campaign_performance")}
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 1, 2
  )
  
  SELECT
    s.business_type,
    
    -- Summary metrics
    s.revenue_30d,
    s.cost_30d,
    s.transactions_30d,
    s.avg_conversion_rate_30d,
    s.avg_roas_30d,
    s.avg_active_clients,
    
    -- Top channels by business type
    ARRAY_AGG(
      STRUCT(
        c.channel_grouping,
        c.channel_revenue,
        c.channel_roas
      )
      ORDER BY c.channel_revenue DESC
      LIMIT 5
    ) AS top_channels
    
  FROM last_30_days_summary s
  LEFT JOIN channel_performance c USING (business_type)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
`);

// Create a simple metrics reference table for Looker Studio
publish("reporting_metrics_definitions", {
  type: "table",
  schema: "reporting",
  tags: ["reporting", "reference"],
  description: "Metric definitions and recommended thresholds by business type"
}).query(ctx => `
  SELECT
    business_type,
    metric_name,
    metric_description,
    good_threshold,
    warning_threshold,
    metric_format
  FROM (
    SELECT 'hotel' AS business_type, 'conversion_rate' AS metric_name, 'Booking conversion rate' AS metric_description, 0.02 AS good_threshold, 0.01 AS warning_threshold, 'percent' AS metric_format
    UNION ALL SELECT 'hotel', 'avg_order_value', 'Average booking value', 5000, 3000, 'currency'
    UNION ALL SELECT 'hotel', 'roas', 'Return on ad spend', 4.0, 2.0, 'ratio'
    UNION ALL SELECT 'hotel', 'cost_per_acquisition', 'Cost per booking', 1500, 2500, 'currency'
    
    UNION ALL SELECT 'beach_club', 'conversion_rate', 'Reservation conversion rate', 0.03, 0.015, 'percent'
    UNION ALL SELECT 'beach_club', 'avg_order_value', 'Average reservation value', 3000, 1500, 'currency'
    UNION ALL SELECT 'beach_club', 'roas', 'Return on ad spend', 5.0, 2.5, 'ratio'
    UNION ALL SELECT 'beach_club', 'cost_per_acquisition', 'Cost per reservation', 1000, 1800, 'currency'
    
    UNION ALL SELECT 'restaurant', 'clicks', 'Ad clicks (no website tracking)', NULL, NULL, 'number'
    UNION ALL SELECT 'restaurant', 'impressions', 'Ad impressions', NULL, NULL, 'number'
    UNION ALL SELECT 'restaurant', 'ctr', 'Click-through rate', 0.03, 0.015, 'percent'
    UNION ALL SELECT 'restaurant', 'avg_cpc', 'Average cost per click', 30, 50, 'currency'
  )
`);