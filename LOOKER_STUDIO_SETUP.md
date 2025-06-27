# Looker Studio Dashboard Setup Guide

## Overview
This guide explains how to connect your unified reporting tables to Looker Studio for cross-client analytics across Hotels, Beach Clubs, and Restaurants.

## Data Sources Created

### 1. Main Unified Table
- **Table:** `reporting.unified_campaign_performance`
- **Purpose:** Single source combining all clients with business type categorization
- **Refresh:** Updates with mart tables (typically daily)

### 2. Business-Specific Views
- **Hotels:** `reporting.reporting_hotel_summary` & `reporting.reporting_hotel_metrics`
- **Beach Club:** `reporting.reporting_beach_club_summary` & `reporting.reporting_beach_club_metrics`  
- **Restaurants:** `reporting.reporting_restaurant_summary` & `reporting.reporting_restaurant_metrics`

### 3. Executive Views
- **Executive Dashboard:** `reporting.reporting_executive_dashboard`
- **Performance Alerts:** `reporting.reporting_performance_alerts`
- **Metrics Reference:** `reporting.reporting_metrics_definitions`

## Looker Studio Setup Instructions

### Step 1: Connect to BigQuery
1. Open Looker Studio
2. Create New Report
3. Add Data Source → BigQuery
4. Select Project: `sublime-elixir-458201-r4`
5. Select Dataset: `reporting`

### Step 2: Recommended Dashboard Structure

#### **Page 1: Executive Overview**
Data Source: `reporting_executive_dashboard`

**Charts to Create:**
- Scorecard: Total Revenue by Business Type (30 days)
- Scorecard: Total ROAS by Business Type
- Bar Chart: Revenue by Business Type
- Line Chart: Daily Revenue Trend (last 30 days)
- Table: Top Channels by Business Type

**Key Metrics:**
- Hotels: Revenue, Bookings, ROAS, Avg Booking Value
- Beach Club: Revenue, Reservations, ROAS, Avg Spend per Visit
- Restaurants: Cost, Clicks, CTR, CPC

#### **Page 2: Hotel Performance**
Data Source: `reporting_hotel_metrics`

**Charts to Create:**
- Scorecard: Total Bookings (30 days)
- Scorecard: Average Booking Value
- Scorecard: Booking Rate
- Line Chart: Daily Bookings by Hotel
- Bar Chart: Revenue by Channel
- Table: Hotel Performance Comparison
- Heatmap: Booking Patterns by Day of Week

**Filters:**
- Client Name (Island Escape, Roost Glamping, Bydlofts Phuket)
- Date Range
- Channel Grouping

#### **Page 3: Beach Club Performance**  
Data Source: `reporting_beach_club_metrics`

**Charts to Create:**
- Scorecard: Total Reservations (30 days)
- Scorecard: Average Spend per Visit
- Scorecard: Upgrade Rate
- Line Chart: Daily Revenue
- Bar Chart: Weekend vs Weekday Performance
- Bar Chart: Monthly Seasonality Trends

**Filters:**
- Date Range
- Day Type (Weekend/Weekday)
- Month

#### **Page 4: Restaurant Performance**
Data Source: `reporting_restaurant_metrics`

**Charts to Create:**
- Scorecard: Total Clicks (30 days)
- Scorecard: Average CTR
- Scorecard: Average CPC
- Line Chart: Daily Clicks by Restaurant
- Bar Chart: Campaign Performance
- Table: Click Performance by Day of Week

**Filters:**
- Client Name (GoFresh Fuel, GoFresh CT)
- Campaign
- Date Range

#### **Page 5: Performance Alerts**
Data Source: `reporting_performance_alerts`

**Charts to Create:**
- Table: Active Alerts (filtered to show only alerts)
- Scorecard: Number of Clients with Alerts
- Bar Chart: Revenue Change by Client
- Bar Chart: ROAS vs Target by Client

### Step 3: Key Metrics by Business Type

#### **Hotels**
- **Primary KPIs:** Bookings, Revenue, Booking Rate, Avg Booking Value
- **Secondary:** ROAS, Cost per Booking, New Guest Revenue %
- **Seasonality:** Day of week patterns, monthly trends

#### **Beach Club**
- **Primary KPIs:** Reservations, Revenue, Avg Spend per Visit
- **Secondary:** Upgrade Rate, Weekend vs Weekday performance
- **Seasonality:** Monthly patterns, day type analysis

#### **Restaurants**
- **Primary KPIs:** Clicks, Impressions, CTR, CPC
- **Secondary:** Cost per Conversion, Campaign Performance
- **Patterns:** Day of week performance, campaign effectiveness

### Step 4: Recommended Filters

#### **Global Filters (All Pages):**
- Date Range (default: Last 30 days)
- Business Type (Hotel, Beach Club, Restaurant)

#### **Page-Specific Filters:**
- Client Name
- Channel Grouping
- Campaign (for detailed analysis)

### Step 5: Color Coding

#### **Business Types:**
- Hotels: Blue (#4285F4)
- Beach Club: Teal (#00ACC1)  
- Restaurants: Orange (#FF9800)

#### **Performance Indicators:**
- Good Performance: Green (#4CAF50)
- Warning: Yellow (#FFC107)
- Alert: Red (#F44336)

### Step 6: Refresh Schedule

The tables update automatically when you run your Dataform pipeline. Recommended refresh in Looker Studio:
- **Frequency:** Daily at 6 AM
- **Cache:** 1 hour for real-time analysis

## Troubleshooting

### Common Issues:
1. **No Data Showing:** Check if Dataform pipeline has run recently
2. **Metrics Not Matching:** Verify date ranges align with Dataform table retention (90 days)
3. **Missing Clients:** Ensure client has data sources configured in `includes/clients.js`

### Data Freshness:
- Staging tables: Updated daily
- Mart tables: Dependent on staging
- Reporting tables: Updated after mart tables complete

## Advanced Features

### Custom Calculated Fields:
```
Efficiency Score = (Revenue / Cost) * Conversion Rate
Growth Rate = (Current Period - Previous Period) / Previous Period
Market Share = Client Revenue / Total Business Type Revenue
```

### Blended Data Sources:
Combine `reporting_executive_dashboard` with `reporting_performance_alerts` for enhanced executive reporting.

## Next Steps

1. Run initial Dataform pipeline to populate reporting tables
2. Set up Looker Studio dashboards using this guide
3. Configure automated refresh schedule
4. Share dashboard with stakeholders
5. Set up email alerts for performance issues