const clients = require("includes/clients.js");

for (const client of clients) {
  if (!client.source_dataset) continue;
  
  publish(`int_${client.name}_conversions_attributed`, {
    type: "table",
    schema: client.output_schema,
    tags: ["intermediate"],
    dependencies: [`stg_${client.name}_ga4_events`, `int_${client.name}_sessions_enriched`],
    bigquery: {
      partitionBy: "conversion_date",
      clusterBy: ["conversion_type", "source", "medium"]
    }
  }).query(ctx => `
    WITH conversion_events AS (
      SELECT
        user_pseudo_id,
        event_timestamp,
        event_datetime,
        DATE(event_datetime) AS conversion_date,
        event_name AS conversion_type,
        session_id,
        
        source,
        medium,
        campaign,
        keyword,
        ad_content,
        
        device.category AS device_category,
        geo.country,
        geo.region,
        geo.city,
        
        ecommerce.transaction_id,
        ecommerce.total_value AS revenue,
        ecommerce.currency,
        ecommerce.tax,
        ecommerce.shipping,
        ecommerce.coupon,
        
        items,
        
        click_ids.gclid,
        
        CASE
          WHEN event_name = 'purchase' THEN 'Purchase'
          WHEN event_name = 'generate_lead' THEN 'Lead'
          WHEN event_name = 'sign_up' THEN 'Signup'
          WHEN event_name = 'first_visit' THEN 'First Visit'
          WHEN event_name LIKE '%book%' THEN 'Booking'
          ELSE 'Other'
        END AS conversion_category,
        
        _dataform_loaded_at
        
      FROM ${ctx.ref(`stg_${client.name}_ga4_events`)}
      WHERE event_name IN (
        'purchase',
        'generate_lead',
        'sign_up',
        'submit_form',
        'complete_booking',
        'add_payment_info'
      )
    ),
    
    attributed_conversions AS (
      SELECT
        c.*,
        s.session_start_time,
        s.session_duration_seconds,
        s.channel_grouping,
        s.attribution_id,
        s.session_number,
        s.pages_viewed,
        s.unique_pages_viewed,
        
        TIMESTAMP_DIFF(c.event_timestamp, s.session_start_timestamp, MINUTE) AS minutes_to_conversion,
        
        ROW_NUMBER() OVER (
          PARTITION BY c.user_pseudo_id 
          ORDER BY c.event_timestamp
        ) AS user_conversion_number,
        
        LAG(c.event_timestamp) OVER (
          PARTITION BY c.user_pseudo_id 
          ORDER BY c.event_timestamp
        ) AS previous_conversion_timestamp
        
      FROM conversion_events c
      LEFT JOIN ${ctx.ref(`int_${client.name}_sessions_enriched`)} s
        ON c.user_pseudo_id = s.user_pseudo_id
        AND c.session_id = s.session_id
    )
    
    SELECT 
      *,
      CASE 
        WHEN previous_conversion_timestamp IS NULL THEN 'New Customer'
        WHEN DATE_DIFF(conversion_date, DATE(DATETIME(TIMESTAMP_MICROS(previous_conversion_timestamp))), DAY) > 90 THEN 'Reactivated'
        ELSE 'Returning Customer'
      END AS customer_type
    FROM attributed_conversions
  `);
}