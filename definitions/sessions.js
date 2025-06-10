// definitions/sessions.js
const clients  = require("includes/clients.js");
const metrics  = require("includes/shared_metrics/metrics.js");

for (const client of clients) {
  publish(`${client.name}_sessions`, {
    type:   "view",
    schema: client.output_schema,
  }).query(ctx => `
    /* ---------- 1. raw events ---------- */
    WITH base_events AS (
      SELECT
        user_pseudo_id,

        -- GA4 session id is stored in event_params
        (SELECT value.int_value
         FROM   UNNEST(event_params)
         WHERE  key = 'ga_session_id')     AS session_id,

        event_timestamp,
        event_name,

        -- marketing dims
        traffic_source.name   AS campaign,
        traffic_source.source AS source,
        traffic_source.medium AS medium,

        ${metrics.device_category}         AS device,
        ${metrics.country}                 AS country
      FROM \`${client.project_id}.${client.source_dataset}.events_*\`
      WHERE event_name IS NOT NULL
    )

    /* ---------- 2. roll up per session ---------- */
    SELECT
      session_id,
      user_pseudo_id,

      MIN(event_timestamp)                            AS session_start,
      MAX(event_timestamp) - MIN(event_timestamp)     AS session_duration,
      COUNT(*)                                        AS event_count,
      MAX(IF(event_name = 'user_engagement',1,0))     AS engaged_session,

      device,
      country,
      campaign,
      source,
      medium
    FROM base_events
    WHERE session_id IS NOT NULL
    GROUP BY
      session_id,
      user_pseudo_id,
      device,
      country,
      campaign,
      source,
      medium
  `);           /* <-- closes query() */
}               /* <-- closes for-loop */
