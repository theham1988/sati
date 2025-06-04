const clients = require("includes/clients.js");
const metrics = require("includes/shared_metrics/metrics.js");

for (const client of clients) {
  publish(`${client.name}_sessions`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    WITH base_events AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(user_properties) WHERE key = "ga_session_id") AS session_id,
        event_timestamp,
        event_name,
        ${metrics.device_category} AS device,
        ${metrics.country} AS country
      FROM \`${client.project_id}.${client.source_dataset}.events_*\`
      WHERE event_name IS NOT NULL
    )

    SELECT
      session_id,
      user_pseudo_id,
      MIN(event_timestamp) AS session_start,
      MAX(event_timestamp) - MIN(event_timestamp) AS session_duration,
      COUNT(*) AS event_count,
      MAX(IF(event_name = 'user_engagement', 1, 0)) AS engaged_session,
      device,
      country
    FROM base_events
    WHERE session_id IS NOT NULL
    GROUP BY session_id, user_pseudo_id, device, country
  `);
}
