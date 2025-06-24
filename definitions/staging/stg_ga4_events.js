const clients = require("includes/clients.js");

for (const client of clients) {
  // Only process clients that have GA4 data configured
  if (!client.source_dataset) continue;
  
  publish(`stg_${client.name}_ga4_events`, {
    type: "table",
    schema: client.output_schema,
    tags: ["staging"],
    bigquery: {
      partitionBy: "DATE(event_timestamp)",
      clusterBy: ["event_name", "user_pseudo_id"]
    }
  }).query(() => `
    WITH flattened_events AS (
      SELECT
        event_timestamp,
        DATETIME(TIMESTAMP_MICROS(event_timestamp)) AS event_datetime,
        event_date,
        event_name,
        event_bundle_sequence_id,
        
        user_pseudo_id,
        user_id,
        
        CONCAT(user_pseudo_id, '-', CAST(event_timestamp AS STRING)) AS unique_event_id,
        
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") AS session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_number") AS session_number,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "engagement_time_msec") AS engagement_time_msec,
        
        traffic_source.source,
        traffic_source.medium,
        traffic_source.name AS campaign,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "term") AS keyword,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "content") AS ad_content,
        
        STRUCT(
          device.category AS category,
          device.mobile_brand_name AS mobile_brand,
          device.mobile_model_name AS mobile_model,
          device.operating_system AS os,
          device.operating_system_version AS os_version,
          device.language AS language,
          device.web_info.browser AS browser,
          device.web_info.browser_version AS browser_version,
          device.web_info.hostname AS hostname
        ) AS device,
        
        STRUCT(
          geo.country AS country,
          geo.region AS region,
          geo.city AS city,
          geo.sub_continent AS sub_continent,
          geo.metro AS metro
        ) AS geo,
        
        STRUCT(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_title") AS page_title,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_referrer") AS page_referrer,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "hostname") AS hostname,
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "entrances") AS entrances,
          (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "exits") AS exits
        ) AS page,
        
        STRUCT(
          ecommerce.transaction_id,
          ecommerce.value AS total_value,
          ecommerce.currency,
          ecommerce.tax,
          ecommerce.shipping,
          ecommerce.coupon,
          ecommerce.payment_type,
          ecommerce.affiliation
        ) AS ecommerce,
        
        ARRAY(
          SELECT AS STRUCT
            item_id,
            item_name,
            item_category,
            item_category2,
            item_category3,
            item_category4,
            item_category5,
            item_brand,
            item_variant,
            price,
            quantity,
            coupon,
            affiliation,
            location_id,
            item_list_name,
            item_list_id,
            index,
            promotion_id,
            promotion_name
          FROM UNNEST(items)
        ) AS items,
        
        STRUCT(
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "gclid") AS gclid,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "dclid") AS dclid,
          (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "srsltid") AS srsltid
        ) AS click_ids,
        
        privacy_info.ads_storage,
        privacy_info.analytics_storage,
        
        _TABLE_SUFFIX AS table_suffix,
        CURRENT_TIMESTAMP() AS _dataform_loaded_at
        
      FROM \`${client.project_id}.${client.source_dataset}.events_*\`
      WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
    )
    
    SELECT * FROM flattened_events
  `);
}