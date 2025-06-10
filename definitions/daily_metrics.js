const clients = require("includes/clients.js");

for (const client of clients) {
  publish(`${client.name}_daily_metrics`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      -- reporting date (UTC) based on session start
      DATE(TIMESTAMP_MICROS(s.session_start))          AS date,

      COUNT(DISTINCT s.session_id)                     AS sessions,
      COUNT(c.event_name)                              AS conversions,
      SUM(c.revenue)                                   AS revenue,

      -- keep marketing breakdowns so you can drill later
      s.campaign,
      s.source,
      s.medium

    FROM  \`${client.project_id}.${client.output_schema}.${client.name}_sessions\`     AS s
    LEFT JOIN \`${client.project_id}.${client.output_schema}.${client.name}_conversions\` AS c
      ON  s.user_pseudo_id = c.user_pseudo_id
      AND DATE(TIMESTAMP_MICROS(s.session_start)) =
          DATE(TIMESTAMP_MICROS(c.event_timestamp))

    GROUP BY
      date, campaign, source, medium
  `);
}
