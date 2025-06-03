const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_first_visit_not_null`, `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_user\`
    WHERE first_visit IS NULL
  `);
}
