const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_session_event_timestamp_valid`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_sessions\`
    WHERE event_timestamp IS NULL
  `);
}
