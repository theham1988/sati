const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_source_medium_not_null`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_sessions\`
    WHERE source IS NULL OR medium IS NULL
  `);
}
