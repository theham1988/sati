const clients = require("includes/clients.js");

for (const client of clients) {
  publish(`${client.name}_conversions`, {
    type: "view",
    schema: client.output_schema
  }).query(ctx => `
    SELECT
      user_pseudo_id,
      event_name,
      event_timestamp,
      traffic_source.source AS source,
      traffic_source.medium AS medium,
      traffic_source.name AS campaign,
      geo.country,
      geo.region,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value") AS revenue,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "currency") AS currency
    FROM \`${client.project_id}.${client.source_dataset}.events_*\`
    WHERE event_name IN ("purchase", "generate_lead")
  `);
}
