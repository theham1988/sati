// definitions/ads_costs.js   – fixed
const clients = require('includes/clients.js');

for (const client of clients) {
  publish(`${client.name}_ads_costs`, {
    type: 'view',
    schema: client.output_schema,
  }).query(ctx => `
    SELECT
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)      AS cost_date,
      metrics.impressions                      AS impressions,
      metrics.ad_clicks                        AS clicks,
      metrics.cost_micros / 1e6               AS ad_cost          -- micros ➜ currency
    FROM \`${ctx.project}.${client.google_ads_dataset}.Ads_Stats_* \`
    WHERE _TABLE_SUFFIX BETWEEN
          FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY))
      AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
  `);
}
