const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_conversion_event_name_valid`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_conversions\`
    WHERE event_name NOT IN ("purchase", "generate_lead")
  `);
}
