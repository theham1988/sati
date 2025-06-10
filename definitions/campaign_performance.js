const clients = require("includes/clients.js");

for (const client of clients) {
  publish(`${client.name}_campaign_performance`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      s.campaign               AS campaign,
      s.source                 AS source,
      s.medium                 AS medium,

      COUNT(DISTINCT s.session_id)            AS sessions,
      COUNT(c.event_name)                     AS conversions,
      SUM(c.revenue)                          AS revenue
      -- ROAS will be added later, once ad_cost exists

    FROM \`${client.project_id}.${client.output_schema}.${client.name}_sessions\`     AS s
    LEFT JOIN \`${client.project_id}.${client.output_schema}.${client.name}_conversions\` AS c
      ON  s.user_pseudo_id = c.user_pseudo_id
      AND DATE(TIMESTAMP_MICROS(s.session_start)) =
          DATE(TIMESTAMP_MICROS(c.event_timestamp))   -- same calendar day

    GROUP BY
      campaign, source, medium
  `);
}
