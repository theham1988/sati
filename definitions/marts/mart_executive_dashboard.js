const clients = require("includes/clients.js");

for (const client of clients) {
  const hasGA4 = client.source_dataset ? true : false;
  const hasGoogleAds = client.google_ads_dataset ? true : false;
  
  // Skip clients with no data sources
  if (!hasGA4 && !hasGoogleAds) continue;
  
  publish(`mart_${client.name}_executive_dashboard`, {
    type: "view",
    schema: client.output_schema,
    tags: ["marts"],
    dependencies: [`mart_${client.name}_campaign_performance`]
  }).query(ctx => `
    WITH current_period AS (
      SELECT
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(transactions) AS total_transactions,
        SUM(sessions) AS total_sessions,
        SUM(users) AS total_users,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS overall_roas,
        SAFE_DIVIDE(SUM(transactions), SUM(sessions)) AS overall_conversion_rate,
        SAFE_DIVIDE(SUM(revenue), SUM(transactions)) AS overall_aov
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ),
    
    previous_period AS (
      SELECT
        SUM(revenue) AS total_revenue,
        SUM(cost) AS total_cost,
        SUM(transactions) AS total_transactions,
        SUM(sessions) AS total_sessions,
        SUM(users) AS total_users,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS overall_roas,
        SAFE_DIVIDE(SUM(transactions), SUM(sessions)) AS overall_conversion_rate,
        SAFE_DIVIDE(SUM(revenue), SUM(transactions)) AS overall_aov
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ),
    
    channel_performance AS (
      SELECT
        channel_grouping,
        SUM(revenue) AS revenue,
        SUM(cost) AS cost,
        SUM(transactions) AS transactions,
        SUM(sessions) AS sessions,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS roas,
        SAFE_DIVIDE(SUM(transactions), SUM(sessions)) AS conversion_rate
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY channel_grouping
    ),
    
    top_campaigns AS (
      SELECT
        campaign,
        channel_grouping,
        SUM(revenue) AS revenue,
        SUM(cost) AS cost,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS roas,
        SUM(transactions) AS transactions
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND campaign IS NOT NULL
      GROUP BY campaign, channel_grouping
      ORDER BY revenue DESC
      LIMIT 10
    ),
    
    daily_trend AS (
      SELECT
        date,
        SUM(revenue) AS daily_revenue,
        SUM(cost) AS daily_cost,
        SUM(transactions) AS daily_transactions,
        SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS daily_roas
      FROM ${ctx.ref(`mart_${client.name}_campaign_performance`)}
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      GROUP BY date
    )
    
    SELECT
      '${client.name}' AS client_name,
      CURRENT_DATE() AS report_date,
      
      cp.total_revenue AS revenue_30d,
      pp.total_revenue AS revenue_prev_30d,
      SAFE_DIVIDE(cp.total_revenue - pp.total_revenue, pp.total_revenue) AS revenue_change,
      
      cp.total_cost AS cost_30d,
      pp.total_cost AS cost_prev_30d,
      SAFE_DIVIDE(cp.total_cost - pp.total_cost, pp.total_cost) AS cost_change,
      
      cp.overall_roas AS roas_30d,
      pp.overall_roas AS roas_prev_30d,
      cp.overall_roas - pp.overall_roas AS roas_change,
      
      cp.total_transactions AS transactions_30d,
      cp.overall_conversion_rate AS conversion_rate_30d,
      cp.overall_aov AS aov_30d,
      
      (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(
        channel_grouping,
        revenue,
        cost,
        roas,
        conversion_rate
      ) ORDER BY revenue DESC)) FROM channel_performance) AS channel_performance_json,
      
      (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(
        campaign,
        channel_grouping,
        revenue,
        roas
      ) ORDER BY revenue DESC)) FROM top_campaigns) AS top_campaigns_json,
      
      (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(
        date,
        daily_revenue,
        daily_cost,
        daily_roas
      ) ORDER BY date)) FROM daily_trend) AS daily_trend_json
      
    FROM current_period cp
    CROSS JOIN previous_period pp
  `);
}