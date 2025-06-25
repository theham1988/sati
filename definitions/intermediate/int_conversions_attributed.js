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
        DATE(TIMESTAMP_MICROS(event_timestamp)) AS conversion_date,
        event_name AS conversion_type,
        session_id,
        
        source,
        medium,
        campaign,
        
        device_category,
        country,
        
        transaction_id,
        revenue,
        currency,
        
        gclid,
        
        CASE
          WHEN event_name = 'purchase' THEN 'Purchase'
          WHEN event_name = 'generate_lead' THEN 'Lead'
          WHEN event_name = 'sign_up' THEN 'Signup'
          WHEN event_name = 'first_visit' THEN 'First Visit'
          WHEN event_name LIKE '%book%' THEN 'Booking'
          ELSE 'Other'
        END AS conversion_category,
        
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
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
        TIMESTAMP_DIFF(TIMESTAMP_MICROS(c.event_timestamp), TIMESTAMP_MICROS(s.session_start_timestamp), MINUTE) AS minutes_to_conversion,
        
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