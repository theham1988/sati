const clients = require("includes/clients.js");

for (const client of clients) {
  // Skip clients without GA4 data
  if (!client.source_dataset) continue;
  
  assert(`${client.name}_source_medium_not_null`, `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_events\`
    WHERE source IS NOT NULL 
      AND source != 'direct'
      AND (medium IS NULL OR medium = '')
  `);
}
