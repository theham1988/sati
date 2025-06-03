const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_user_id_not_null`, `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_user\`
    WHERE user_pseudo_id IS NULL
  `);
}
