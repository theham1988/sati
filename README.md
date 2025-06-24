# Phuket Hospitality Marketing Analytics Platform

AI-powered marketing analytics platform for hospitality businesses in Phuket, providing unified ROAS tracking across GA4 and Google Ads data with advanced attribution and performance insights.

## Project Structure

```
sati/
├── workflow_settings.yaml        # Workflow settings
├── includes/
│   └── clients.js               # Client configurations
├── definitions/                 # SQL transformations
│   ├── staging/                 # Raw data ingestion
│   │   ├── stg_ga4_events.js   # Flattened GA4 events
│   │   └── stg_google_ads_cost.js # Google Ads performance
│   ├── intermediate/            # Business logic layer
│   │   ├── int_sessions_enriched.js # Session aggregations
│   │   └── int_conversions_attributed.js # Conversion attribution
│   ├── marts/                   # Business-ready datasets
│   │   ├── mart_campaign_performance.js # ROAS & metrics
│   │   └── mart_executive_dashboard.js # Executive KPIs
│   └── backup/                  # Legacy transformations
└── assertions/                  # Data quality tests
    ├── user_id_not_null.js
    ├── conversion_currency_and_revenue_not_null.js
    ├── device_and_country_populated.js
    ├── first_visit_not_null.js
    ├── session_event_timestamp_valid.js
    ├── source_medium_not_null.js
    └── event_timestamp_valid.js
```

## Configuration

### Clients Configuration (`includes/clients.js`)

Each client is configured with the following properties:

- `name`: Client identifier
- `project_id`: BigQuery project ID
- `source_dataset`: GA4 dataset name (null if no GA4 data)
- `output_schema`: Output dataset name
- `google_ads_dataset`: Google Ads dataset (optional)
- `business_type`: Business type (e.g., "restaurant")
- `has_website`: Whether client has a website

### Current Clients

1. **barracuda_phuket** - Hotel/Resort
2. **island_escape** - Hotel/Resort  
3. **roost_glamping** - Glamping
4. **bydlofts_phuket** - Hotel/Resort
5. **gofresh_fuel** - Restaurant (no website)
6. **gofresh_ct** - Restaurant (no website)

## Data Architecture

### Staging Layer (`definitions/staging/`)

#### 1. GA4 Events (`stg_ga4_events.js`)
- Flattens nested GA4 event data structure
- Extracts all relevant event parameters
- Implements incremental loading with timestamp watermarks
- Partitioned by event date for performance
- Includes e-commerce data, click IDs (GCLID), and custom parameters

#### 2. Google Ads Cost (`stg_google_ads_cost.js`)
- Ingests ad performance data from Google Ads
- Tracks cost, clicks, impressions at ad group level
- Supports incremental daily updates
- Links to GA4 data via GCLID for attribution

### Intermediate Layer (`definitions/intermediate/`)

#### 1. Enriched Sessions (`int_sessions_enriched.js`)
- Aggregates events into session-level metrics
- Calculates session duration, bounce rate, pages viewed
- Applies channel grouping logic (Google Ads, Facebook, Organic, etc.)
- Identifies conversion sessions and engagement patterns
- Creates attribution IDs for ROAS calculation

#### 2. Attributed Conversions (`int_conversions_attributed.js`)
- Identifies and categorizes conversion events
- Links conversions to originating sessions
- Classifies customers (New, Returning, Reactivated)
- Calculates time-to-conversion metrics
- Preserves full e-commerce transaction details

### Marts Layer (`definitions/marts/`)

#### 1. Campaign Performance (`mart_campaign_performance.js`)
- Daily granularity with full attribution
- Unified view of GA4 sessions and Google Ads costs
- Calculates key metrics:
  - ROAS (Return on Ad Spend)
  - CPA (Cost Per Acquisition)
  - Conversion rates
  - Average order value
  - New customer revenue ratio
- Handles both scenarios: with and without Google Ads data

#### 2. Executive Dashboard (`mart_executive_dashboard.js`)
- High-level KPIs with period-over-period comparisons
- JSON aggregations for easy API consumption:
  - Channel performance breakdown
  - Top 10 campaigns by revenue
  - 30-day trend analysis
- Designed for real-time dashboard integration

## Data Quality Assertions

The project includes comprehensive data quality checks:

- **User ID validation**: Ensures user_pseudo_id is not null
- **Conversion data validation**: Validates revenue and currency for conversions
- **Device and country validation**: Ensures device and geographic data is populated
- **First visit validation**: Checks first visit data completeness
- **Timestamp validation**: Validates event timestamps are within reasonable bounds
- **Traffic source validation**: Ensures source/medium consistency

## Setup and Usage

### Prerequisites

1. **BigQuery Access**: Ensure you have access to the GA4 BigQuery datasets
2. **Dataform CLI**: Install Dataform CLI
3. **Authentication**: Set up Google Cloud authentication

### Installation

```bash
# Install Dataform CLI
npm install -g @dataform/cli

# Authenticate with Google Cloud
gcloud auth application-default login
```

### Running the Project

```bash
# Compile the project
dataform compile

# Initial setup - run staging tables first
dataform run --tags staging

# Run intermediate transformations
dataform run --tags intermediate

# Run mart tables
dataform run --tags marts

# Run all transformations
dataform run

# Run specific client transformations
dataform run --actions "*barracuda_phuket*"

# Run with full refresh (for initial load)
dataform run --full-refresh
```

### Adding a New Client

1. Add client configuration to `includes/clients.js`:
```javascript
{
  name: "new_client",
  project_id: "your-project-id",
  source_dataset: "analytics_123456789",
  output_schema: "new_client",
  business_type: "hotel",
  has_website: true
}
```

2. The transformations will automatically be generated for the new client.

## Data Retention

- Event data is limited to the last 90 days for performance
- User and session data includes historical information
- Daily metrics are calculated from the 90-day window

## Performance Considerations

- Tables use partitioning by date where applicable
- Views are used instead of tables for flexibility
- 90-day data window balances performance and data completeness

## Monitoring

Monitor the following key metrics:
- Data freshness (last updated timestamps)
- Assertion failures (data quality issues)
- Query performance and costs
- Client-specific conversion rates

## Troubleshooting

### Common Issues

1. **Missing GA4 Data**: Clients without `source_dataset` will be skipped
2. **Authentication Errors**: Ensure proper Google Cloud authentication
3. **Permission Errors**: Verify BigQuery dataset access
4. **Assertion Failures**: Check data quality and source data integrity

### Debugging

```bash
# Check compilation errors
dataform compile --verbose

# Test specific queries
dataform run --actions "client_name_events" --dry-run

# View generated SQL
dataform compile --output-dir ./compiled
```

## ROAS Optimization Features

### Attribution Methodology
- **GCLID Matching**: Direct attribution for Google Ads clicks
- **Session-Based Attribution**: Links costs to converting sessions
- **Multi-Touch Support**: Tracks user journey across sessions
- **Channel Grouping**: Unified performance view across platforms

### Key Metrics Calculated
1. **ROAS (Return on Ad Spend)**: Revenue / Cost ratio
2. **CPA (Cost Per Acquisition)**: Cost / Conversions
3. **LTV Indicators**: New vs returning customer revenue
4. **Engagement Metrics**: Session duration, pages viewed, bounce rate
5. **Conversion Velocity**: Time from first touch to conversion

### Use Cases
- **Budget Optimization**: Identify high-ROAS campaigns for scaling
- **Channel Mix Analysis**: Compare performance across marketing channels  
- **Customer Journey Insights**: Understand conversion paths
- **Seasonality Tracking**: Monitor performance trends over time

## Query Examples

### Get Current Month ROAS by Channel
```sql
SELECT 
  channel_grouping,
  SUM(revenue) as total_revenue,
  SUM(cost) as total_cost,
  SAFE_DIVIDE(SUM(revenue), SUM(cost)) as roas
FROM `project.dataset.mart_clientname_campaign_performance`
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
GROUP BY channel_grouping
ORDER BY total_revenue DESC
```

### Find Underperforming Campaigns
```sql
SELECT 
  campaign,
  channel_grouping,
  SUM(cost) as total_cost,
  SUM(revenue) as total_revenue,
  SAFE_DIVIDE(SUM(revenue), SUM(cost)) as roas
FROM `project.dataset.mart_clientname_campaign_performance`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND cost > 100  -- Minimum spend threshold
GROUP BY campaign, channel_grouping
HAVING roas < 2  -- Below target ROAS
ORDER BY total_cost DESC
```

## Future Enhancements
1. **Predictive ROAS Modeling**: ML-based campaign performance forecasting
2. **Automated Bid Recommendations**: Real-time bidding strategy suggestions
3. **Cross-Property Analytics**: Unified view across all Phuket properties
4. **WhatsApp/LINE Integration**: Track messaging app conversions
5. **Competitor Intelligence**: Market share and pricing insights

## Contributing

When adding new transformations or assertions:
1. Follow the existing naming conventions
2. Include proper documentation
3. Add appropriate data quality checks
4. Test with multiple clients
5. Consider performance implications

## Support

For issues or questions:
1. Check the Dataform documentation
2. Review BigQuery GA4 schema documentation
3. Test with a single client first
4. Validate data quality assertions 