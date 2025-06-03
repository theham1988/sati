const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_device_and_country_populated`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_user\`
    WHERE device IS NULL OR country IS NULL
  `);
}
