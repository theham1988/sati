const clients = require("includes/clients.js");

for (const client of clients) {
  publish(`${client.name}_sessions`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      user_pseudo_id,
      event_bundle_sequence_id,
      event_timestamp,
      traffic_source.source AS source,
      traffic_source.medium AS medium,
      traffic_source.name AS campaign,
      device.category AS device_category,
      geo.country,
      geo.region,
      event_date,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE event_name = "session_start"
  `);
}
