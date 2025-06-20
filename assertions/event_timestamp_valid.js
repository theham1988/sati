const clients = require("includes/clients.js");

for (const client of clients) {
  // Skip clients without GA4 data
  if (!client.source_dataset) continue;
  
  assert(`${client.name}_event_timestamp_valid`, `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_events\`
    WHERE event_timestamp IS NULL 
       OR event_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 YEAR)
       OR event_timestamp > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  `);
} 