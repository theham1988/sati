# Deployment Sequence for Dataform Web Interface

## Initial Deployment (First Time)

Run these commands in sequence in the Dataform web interface:

### Step 1: Deploy Staging Tables
```
Run with tags: staging
Options: Full refresh
```
This creates:
- stg_*_ga4_events (4 tables)
- stg_*_google_ads_cost (3 tables)

### Step 2: Deploy Intermediate Tables  
```
Run with tags: intermediate
```
This creates:
- int_*_sessions_enriched (4 tables)
- int_*_conversions_attributed (4 tables)

### Step 3: Deploy Mart Tables
```
Run with tags: marts
```
This creates:
- mart_*_campaign_performance (6 tables) 
- mart_*_executive_dashboard (6 views)

### Step 4: Run Data Quality Checks
```
Run assertions only
```

## Daily Operations

### Incremental Updates
1. Run staging tables (incremental)
2. Run intermediate + marts tables

### Scheduling Recommendation
- **Morning (3 AM)**: Staging tables
- **Morning (4 AM)**: Intermediate + Marts tables

## Troubleshooting

### If you get "Table not found" errors:
1. **Check dependencies**: Make sure staging tables exist before running intermediate
2. **Use full refresh**: For initial deployment of any new tables
3. **Run by layer**: staging → intermediate → marts

### Common Issues:
- **Missing staging tables**: Run staging layer first with full refresh
- **Dataset not found**: Verify clients.js matches actual BigQuery datasets
- **Permission errors**: Check service account has access to all datasets