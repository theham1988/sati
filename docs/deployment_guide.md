# Deployment Guide

## Prerequisites
1. Google Cloud authentication configured
2. Access to BigQuery datasets
3. Dataform CLI installed

## Initial Setup

### 1. Authenticate with Google Cloud
```bash
gcloud auth application-default login
```

### 2. Verify Compilation
```bash
dataform compile
```

## Deployment Steps

### Step 1: Deploy Staging Tables (Initial Load)
```bash
# Full refresh for initial historical data load
dataform run --tags staging --full-refresh

# This creates:
# - stg_*_ga4_events (for clients with GA4)
# - stg_*_google_ads_cost (for clients with Google Ads)
```

### Step 2: Deploy Intermediate Tables
```bash
# Run intermediate transformations
dataform run --tags intermediate

# This creates:
# - int_*_sessions_enriched
# - int_*_conversions_attributed
```

### Step 3: Deploy Mart Tables
```bash
# Run mart transformations
dataform run --tags marts

# This creates:
# - mart_*_campaign_performance
# - mart_*_executive_dashboard
```

### Step 4: Run Data Quality Checks
```bash
# Run all assertions
dataform run --actions "*assertions*"
```

## Daily Operations

### Incremental Updates
```bash
# Run daily incremental updates
dataform run --tags staging
dataform run --tags intermediate,marts
```

### Monitoring
1. Check for assertion failures
2. Monitor query costs in BigQuery
3. Verify data freshness with _dataform_loaded_at

## Client-Specific Deployment

### Deploy Single Client
```bash
# Example for barracuda_phuket
dataform run --actions "*barracuda_phuket*"
```

### Deploy by Business Type
```bash
# Hotels with GA4 + Google Ads
dataform run --actions "*barracuda_phuket*,*bydlofts_phuket*"

# Restaurants (Google Ads only)
dataform run --actions "*gofresh*"
```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```bash
   gcloud auth application-default login
   ```

2. **Missing Tables**
   - Verify source datasets exist in BigQuery
   - Check client configuration in includes/clients.js

3. **Assertion Failures**
   - Review data quality in source
   - Check for NULL values in critical fields

4. **Performance Issues**
   - Ensure partitioning is working
   - Consider reducing date range for initial loads

## Production Schedule

### Recommended Cron Schedule
```cron
# Daily at 3 AM UTC
0 3 * * * cd /path/to/project && dataform run --tags staging
30 3 * * * cd /path/to/project && dataform run --tags intermediate,marts
```

### BigQuery Scheduled Queries Alternative
1. Export compiled SQL from Dataform
2. Create scheduled queries in BigQuery Console
3. Set up alerting for failures

## Cost Optimization

1. **Use Incremental Loading**: Already configured for staging tables
2. **Partition Pruning**: Query specific date ranges
3. **Clustering**: Tables are clustered by key dimensions
4. **Monitor Slot Usage**: Check BigQuery slot consumption

## Next Steps After Deployment

1. **Connect BI Tools**
   - Use mart_*_executive_dashboard for dashboards
   - Query mart_*_campaign_performance for detailed analysis

2. **Set Up Alerts**
   - ROAS below threshold
   - Sudden cost spikes
   - Conversion rate drops

3. **API Integration**
   - Executive dashboard JSON fields ready for API
   - Consider Cloud Functions for real-time access