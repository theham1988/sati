const clients = require("includes/clients.js");

for (const client of clients) {
  publish(`${client.name}_campaign_performance`, {
    type: "view",
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      s.campaign   AS campaign,
      s.source     AS source,
      s.medium     AS medium,
      COUNT(DISTINCT s.session_start)                       AS sessions,
      COUNT(c.event_name)                                   AS conversions,
      SUM(c.revenue)                                        AS revenue,  
      SAFE_DIVIDE(SUM(c.revenue), SUM(s.ad_cost))           AS roas
    FROM \`${client.project_id}.${client.output_schema}.${client.name}_sessions\`   AS s
    LEFT JOIN \`${client.project_id}.${client.output_schema}.${client.name}_conversions\` AS c
      ON  s.user_pseudo_id = c.user_pseudo_id          -- same user
      AND DATE(TIMESTAMP_MICROS(s.session_start)) =         -- same calendar day
         DATE(TIMESTAMP_MICROS(c.event_timestamp))
      GROUP BY
        campaign, source, medium
  `);
}
