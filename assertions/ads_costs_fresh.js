// Assertions ▸ ads_costs_fresh.js
const clients = require('includes/clients.js');

for (const client of clients) {
  assert(`${client.name}_ads_costs_fresh`, `
    SELECT 1
    FROM   \`${client.project_id}.${client.output_schema}.${client.name}_ads_costs\`
    WHERE  cost_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    LIMIT 1
  `);
}
