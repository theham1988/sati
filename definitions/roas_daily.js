const clients = require('includes/clients.js');

for (const client of clients) {
  publish(`${client.name}_roas_daily`, {
    type: 'view',
    schema: client.output_schema,
  }).query(ctx => `
    WITH revenue AS (
      SELECT
        DATE(event_timestamp/1000, "UTC")               AS the_date,
        SUM(
          (SELECT value.int_value
           FROM UNNEST(event_params)
           WHERE key = 'value')
        ) AS revenue,
        COUNTIF(event_name = 'purchase')                AS conversions
      FROM \`${ctx.project}.${client.output_schema}.${client.name}_conversions\`
      GROUP BY the_date
    ),
    cost AS (
      SELECT
        cost_date                                      AS the_date,
        SUM(ad_cost)                                   AS cost
      FROM \`${client.project_id}.${client.output_schema}.${client.name}_ads_costs\`
      GROUP BY the_date
    )
    SELECT
      c.the_date                                       AS date,
      COALESCE(r.revenue, 0)                           AS revenue,
      c.cost,
      COALESCE(r.conversions, 0)                       AS conversions,
      SAFE_DIVIDE(r.revenue, c.cost)                   AS roas,
      SAFE_DIVIDE(c.cost, r.conversions)               AS cpa
    FROM cost c
    LEFT JOIN revenue r USING(the_date)
    ORDER BY date DESC
  `);
}
