module.exports = {
  revenue: `(SELECT value.int_value FROM UNNEST(event_params) WHERE key = "value")`,
  currency: `(SELECT value.string_value FROM UNNEST(event_params) WHERE key = "currency")`,
  campaign: `traffic_source.name`,
  medium: `traffic_source.medium`,
  source: `traffic_source.source`,
  device_category: `device.category`,
  country: `geo.country`,
  region: `geo.region`
};
