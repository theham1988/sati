const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_conversion_currency_and_revenue_not_null`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_conversions\`
    WHERE currency IS NULL OR revenue IS NULL
  `);
}
