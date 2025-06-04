const clients = require("includes/clients.js");
const metrics = require("includes/shared_metrics/metrics.js");

for (const client of clients) {
  publish(`${client.name}_sessions`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      session_id,
      user_pseudo_id,
      MIN(event_timestamp) AS session_start,
      MAX(event_timestamp) - MIN(event_timestamp) AS session_duration,
      COUNT(*) AS event_count,
      MAX(IF(event_name = 'user_engagement', 1, 0)) AS engaged_session,
      ${metrics.device_category} AS device,
      ${metrics.country} AS country
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE session_id IS NOT NULL
    GROUP BY session_id, user_pseudo_id, ${metrics.device_category}, ${metrics.country}
  `);
}
