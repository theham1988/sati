const clients = require("includes/clients.js");

for (const client of clients) {
  assert(`${client.name}_first_touch_timestamp_not_null`, ctx => `
    SELECT *
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_user\`
    WHERE user_first_touch_timestamp IS NULL
  `);
}
