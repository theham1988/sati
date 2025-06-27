// Hotel-specific metrics focusing on booking patterns and revenue
publish("reporting_hotel_metrics", {
  type: "view",
  schema: "reporting", 
  tags: ["reporting", "hotel", "metrics"],
  description: "Hotel-specific KPIs and metrics for Looker Studio"
}).query(ctx => `
  WITH hotel_data AS (
    SELECT
      client_name,
      date,
      channel_grouping,
      
      -- Core hotel metrics
      revenue,
      transactions AS bookings,
      sessions,
      users AS unique_visitors,
      new_customer_revenue,
      cost,
      
      -- Calculated metrics
      SAFE_DIVIDE(revenue, transactions) AS avg_booking_value,
      SAFE_DIVIDE(transactions, sessions) AS booking_rate,
      SAFE_DIVIDE(new_customer_revenue, revenue) AS new_guest_revenue_ratio,
      SAFE_DIVIDE(revenue, cost) AS roas,
      avg_minutes_to_conversion / 60 AS avg_hours_to_booking
      
    FROM ${ctx.ref("unified_campaign_performance")}
    WHERE business_type = 'hotel'
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  ),
  
  weekly_seasonality AS (
    SELECT
      client_name,
      EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
      AVG(bookings) AS avg_daily_bookings,
      AVG(revenue) AS avg_daily_revenue
    FROM hotel_data
    GROUP BY 1, 2
  ),
  
  channel_effectiveness AS (
    SELECT
      client_name,
      channel_grouping,
      SUM(bookings) AS total_bookings,
      SUM(revenue) AS total_revenue,
      AVG(booking_rate) AS avg_booking_rate,
      AVG(avg_booking_value) AS avg_booking_value,
      SAFE_DIVIDE(SUM(revenue), SUM(cost)) AS channel_roas
    FROM hotel_data
    WHERE cost > 0
    GROUP BY 1, 2
  )
  
  SELECT
    h.*,
    w.avg_daily_bookings AS typical_bookings_for_day,
    c.channel_roas AS channel_specific_roas
  FROM hotel_data h
  LEFT JOIN weekly_seasonality w 
    ON h.client_name = w.client_name 
    AND EXTRACT(DAYOFWEEK FROM h.date) = w.day_of_week
  LEFT JOIN channel_effectiveness c
    ON h.client_name = c.client_name
    AND h.channel_grouping = c.channel_grouping
`);

// Beach club metrics focusing on reservations and seasonality
publish("reporting_beach_club_metrics", {
  type: "view",
  schema: "reporting",
  tags: ["reporting", "beach_club", "metrics"],
  description: "Beach club-specific KPIs focusing on reservations and upgrades"
}).query(ctx => `
  WITH beach_club_data AS (
    SELECT
      client_name,
      date,
      channel_grouping,
      
      -- Core metrics
      revenue,
      transactions AS reservations,
      sessions,
      users,
      leads AS upgrade_purchases,  -- Assuming upgrades tracked as leads
      cost,
      
      -- Calculated metrics
      SAFE_DIVIDE(revenue, transactions) AS avg_spend_per_visit,
      SAFE_DIVIDE(transactions, sessions) AS reservation_rate,
      SAFE_DIVIDE(leads, transactions) AS upgrade_rate,
      SAFE_DIVIDE(revenue, cost) AS roas,
      
      -- Time-based patterns
      EXTRACT(MONTH FROM date) AS month,
      EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
      CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
      END AS day_type
      
    FROM ${ctx.ref("unified_campaign_performance")}
    WHERE business_type = 'beach_club'
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  ),
  
  monthly_trends AS (
    SELECT
      client_name,
      month,
      AVG(reservations) AS avg_daily_reservations,
      AVG(revenue) AS avg_daily_revenue,
      AVG(upgrade_rate) AS avg_upgrade_rate
    FROM beach_club_data
    GROUP BY 1, 2
  ),
  
  weekend_vs_weekday AS (
    SELECT
      client_name,
      day_type,
      AVG(reservations) AS avg_reservations,
      AVG(revenue) AS avg_revenue,
      AVG(avg_spend_per_visit) AS avg_spend
    FROM beach_club_data
    GROUP BY 1, 2
  )
  
  SELECT
    b.*,
    m.avg_daily_reservations AS typical_monthly_reservations,
    m.avg_upgrade_rate AS typical_monthly_upgrade_rate,
    w.avg_spend AS typical_spend_for_day_type
  FROM beach_club_data b
  LEFT JOIN monthly_trends m
    ON b.client_name = m.client_name
    AND b.month = m.month
  LEFT JOIN weekend_vs_weekday w
    ON b.client_name = w.client_name
    AND b.day_type = w.day_type
`);

// Restaurant metrics (Google Ads only)
publish("reporting_restaurant_metrics", {
  type: "view",
  schema: "reporting",
  tags: ["reporting", "restaurant", "metrics"],
  description: "Restaurant-specific metrics from Google Ads"
}).query(ctx => `
  WITH restaurant_data AS (
    SELECT
      client_name,
      date,
      campaign,
      
      -- Google Ads metrics
      cost,
      clicks,
      impressions,
      transactions AS conversions,  -- From Google Ads conversion tracking
      revenue AS conversion_value,
      
      -- Calculated metrics
      SAFE_DIVIDE(clicks, impressions) AS ctr,
      SAFE_DIVIDE(cost, clicks) AS cpc,
      SAFE_DIVIDE(cost, transactions) AS cost_per_conversion,
      SAFE_DIVIDE(revenue, cost) AS roas,
      
      -- Day patterns
      EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
      EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) AS hour_of_day  -- Would need actual data
      
    FROM ${ctx.ref("unified_campaign_performance")}
    WHERE business_type = 'restaurant'
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  ),
  
  campaign_performance AS (
    SELECT
      client_name,
      campaign,
      SUM(clicks) AS total_clicks,
      SUM(impressions) AS total_impressions,
      SUM(cost) AS total_cost,
      AVG(ctr) AS avg_ctr,
      AVG(cpc) AS avg_cpc,
      SAFE_DIVIDE(SUM(conversions), SUM(clicks)) AS conversion_rate
    FROM restaurant_data
    GROUP BY 1, 2
    HAVING total_cost > 100  -- Filter out low-spend campaigns
  ),
  
  daily_patterns AS (
    SELECT
      client_name,
      day_of_week,
      AVG(clicks) AS avg_daily_clicks,
      AVG(cost) AS avg_daily_cost,
      AVG(ctr) AS avg_daily_ctr
    FROM restaurant_data
    GROUP BY 1, 2
  )
  
  SELECT
    r.*,
    c.avg_ctr AS campaign_avg_ctr,
    c.conversion_rate AS campaign_conversion_rate,
    d.avg_daily_clicks AS typical_clicks_for_day
  FROM restaurant_data r
  LEFT JOIN campaign_performance c
    ON r.client_name = c.client_name
    AND r.campaign = c.campaign
  LEFT JOIN daily_patterns d
    ON r.client_name = d.client_name
    AND r.day_of_week = d.day_of_week
`);

// Unified alerts and anomaly detection
publish("reporting_performance_alerts", {
  type: "view",
  schema: "reporting",
  tags: ["reporting", "alerts"],
  description: "Performance alerts and anomaly detection across all clients"
}).query(ctx => `
  WITH recent_performance AS (
    SELECT
      client_name,
      business_type,
      
      -- Last 7 days metrics
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN revenue ELSE 0 END) AS revenue_7d,
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN cost ELSE 0 END) AS cost_7d,
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN transactions ELSE 0 END) AS transactions_7d,
      
      -- Previous 7 days metrics
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
               AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN revenue ELSE 0 END) AS revenue_prev_7d,
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
               AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN cost ELSE 0 END) AS cost_prev_7d,
      SUM(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
               AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN transactions ELSE 0 END) AS transactions_prev_7d,
      
      -- 30-day averages
      AVG(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN SAFE_DIVIDE(revenue, cost) END) AS avg_roas_30d,
      AVG(CASE WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN conversion_rate END) AS avg_conversion_rate_30d
      
    FROM ${ctx.ref("unified_campaign_performance")}
    GROUP BY 1, 2
  ),
  
  threshold_checks AS (
    SELECT
      r.*,
      m.good_threshold,
      m.warning_threshold,
      
      -- Calculate changes
      SAFE_DIVIDE(revenue_7d - revenue_prev_7d, revenue_prev_7d) AS revenue_change,
      SAFE_DIVIDE(transactions_7d - transactions_prev_7d, transactions_prev_7d) AS transaction_change,
      SAFE_DIVIDE(cost_7d, revenue_7d) AS current_cost_ratio,
      
      -- Alert conditions
      CASE
        WHEN SAFE_DIVIDE(revenue_7d - revenue_prev_7d, revenue_prev_7d) < -0.2 THEN 'Revenue down >20%'
        WHEN SAFE_DIVIDE(transactions_7d - transactions_prev_7d, transactions_prev_7d) < -0.2 THEN 'Transactions down >20%'
        WHEN avg_roas_30d < m.warning_threshold THEN 'ROAS below threshold'
        WHEN cost_7d > revenue_7d * 0.5 THEN 'Cost ratio too high'
        ELSE 'Normal'
      END AS alert_status
      
    FROM recent_performance r
    LEFT JOIN ${ctx.ref("reporting_metrics_definitions")} m
      ON r.business_type = m.business_type
      AND m.metric_name = 'roas'
  )
  
  SELECT
    client_name,
    business_type,
    alert_status,
    revenue_7d,
    revenue_change,
    transaction_change,
    avg_roas_30d,
    good_threshold AS roas_target,
    current_cost_ratio
  FROM threshold_checks
  WHERE alert_status != 'Normal'
  ORDER BY 
    CASE alert_status
      WHEN 'Revenue down >20%' THEN 1
      WHEN 'Transactions down >20%' THEN 2
      WHEN 'ROAS below threshold' THEN 3
      WHEN 'Cost ratio too high' THEN 4
    END,
    revenue_7d DESC
`);