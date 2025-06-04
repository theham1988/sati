const clients = require("includes/clients.js");
const metrics = require("includes/shared_metrics/metrics.js");

for (const client of clients) {
  publish(`${client.name}_user`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      user_pseudo_id,
      MIN(event_timestamp) FILTER (WHERE event_name = 'first_visit') AS first_visit,
      COUNT(DISTINCT event_name) AS event_count,
      COUNT(DISTINCT event_date) AS active_days,
      ${metrics.device_category} AS device,
      ${metrics.country} AS country
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    GROUP BY user_pseudo_id
  `);
}
