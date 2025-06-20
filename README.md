# GA4 BigQuery Dataform Project

This Dataform project transforms Google Analytics 4 (GA4) BigQuery data exports for multiple clients, providing standardized analytics tables and data quality checks.

## Project Structure

```
sati/
├── dataform.json                 # Dataform configuration
├── workflow_settings.yaml        # Workflow settings
├── includes/
│   └── clients.js               # Client configurations
├── definitions/                 # SQL transformations
│   ├── user.js                 # User-level aggregations
│   ├── sessions.js             # Session-level data
│   ├── events.js               # Raw event data
│   ├── conversions.js          # Conversion events
│   └── daily_metrics.js        # Daily aggregated metrics
└── assertions/                 # Data quality tests
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

## Data Transformations

### 1. User Table (`user.js`)
Comprehensive user-level aggregations including:
- First and last visit information
- Engagement metrics (events, sessions, active days)
- Conversion metrics (purchases, leads, revenue)
- Device and location preferences
- Traffic source preferences
- User segmentation (frequency, type)

### 2. Sessions Table (`sessions.js`)
Session-level data including:
- Session identification
- Traffic source information
- Device and geographic data
- Page location

### 3. Events Table (`events.js`)
Raw event data with comprehensive parameters:
- Event identification and timing
- User and session information
- Traffic source details
- Device and geographic information
- Page information
- E-commerce parameters
- Form and engagement metrics

### 4. Conversions Table (`conversions.js`)
Conversion events (purchases, leads) with:
- Revenue and currency information
- Traffic source attribution
- Geographic data

### 5. Daily Metrics Table (`daily_metrics.js`)
Daily aggregated KPIs including:
- User and session metrics
- Conversion metrics
- Revenue tracking
- Traffic source breakdown
- Device and geographic distribution
- Calculated rates and percentages

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

# Run assertions (data quality tests)
dataform run --actions assertions

# Run all transformations
dataform run

# Run specific client transformations
dataform run --actions "barracuda_phuket_*"
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