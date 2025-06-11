// Assertions ▸ daily_metrics_has_rows.js
const clients = require('includes/clients.js');

for (const client of clients) {
  assert(`${client.name}_daily_metrics_has_rows`, `
    SELECT 1
    FROM   \`${client.project_id}.${client.output_schema}.${client.name}_daily_metrics\`
    WHERE  date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    LIMIT 1
  `);
}
