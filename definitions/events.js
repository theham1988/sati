const clients = require("includes/clients.js");

for (const client of clients) {
  // Skip clients without GA4 data
  if (!client.source_dataset) continue;
  
  publish(`${client.name}_events`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      -- Event identification
      event_timestamp,
      event_date,
      event_name,
      event_bundle_sequence_id,
      
      -- User identification
      user_pseudo_id,
      user_id,
      
      -- Session information
      session_id,
      
      -- Traffic source
      traffic_source.source,
      traffic_source.medium,
      traffic_source.name AS campaign,
      traffic_source.term AS keyword,
      
      -- Device information
      device.category AS device_category,
      device.mobile_brand_name,
      device.mobile_model_name,
      device.operating_system,
      device.operating_system_version,
      device.web_info.browser,
      device.web_info.browser_version,
      
      -- Geographic information
      geo.country,
      geo.region,
      geo.city,
      
      -- Page information
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_title") AS page_title,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_referrer") AS page_referrer,
      
      -- E-commerce parameters
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value") AS value,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "currency") AS currency,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "transaction_id") AS transaction_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "item_id") AS item_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "item_name") AS item_name,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "item_category") AS item_category,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "quantity") AS quantity,
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = "price") AS price,
      
      -- Form and engagement parameters
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "form_id") AS form_id,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "form_name") AS form_name,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "engagement_time_msec") AS engagement_time_msec,
      
      -- Custom parameters (you can add more as needed)
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "custom_parameter_1") AS custom_parameter_1,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "custom_parameter_2") AS custom_parameter_2
      
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
  `);
} 