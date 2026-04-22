# B2B Sales Pipeline Analysis

**MavenTech converts 63.2% of engaged prospects to closed-won deals, but only 48.2% of all 8,800 opportunities ever reach a Win. Analysis reveals the critical leak is at the Engaging stage, where 2,473 deals — representing an estimated $5.85M in potential revenue — were ultimately Lost. Using BigQuery for data modeling and Looker Studio for visualization, this project pinpoints coaching and product-mix opportunities that could recover $1–2M in annual revenue.**

> **View Dashboard:** [Looker Studio Report](https://datastudio.google.com/reporting/c66aa14a-55a7-4020-86aa-724349ed2863)

> **View Executive Memo:** [Project_Insights_VP.pdf](./Project_Insights_VP.pdf)

---

## Business Problem

Leadership at MavenTech needs visibility into three areas across ~8,800 CRM opportunities spanning October 2016 – December 2017:

1. **Where deals die in the funnel** — which stage leaks the most value
2. **Which reps / products / regions underperform** — relative to company averages
3. **How the sales cycle is trending** — seasonality, cohort quality over time

---

## Scope Limits

| In Scope | Out of Scope |
|---|---|
| Sales Funnel Analysis (stage conversion rates) | Marketing CAC analysis (no ad spend data) |
| Deal Cohort Analysis (win rate by month opened) | Subscription Churn analysis (no recurring billing) |
| Rep benchmarking vs. internal average | Attribution modeling |
| Product & region revenue segmentation | Customer lifetime value (CLV) |

---

## Core Metrics Defined

| Metric | Definition |
|---|---|
| **Win Rate** | Won ÷ (Won + Lost) — excludes still-open deals |
| **ACV (Avg Contract Value)** | Average `close_value` of Won deals |
| **Sales Cycle Length** | `DATE_DIFF(close_date, engage_date, DAY)` for Won deals |
| **Stage Conversion Rate** | Deals advancing to next stage ÷ deals entering current stage |
| **Weighted Pipeline** | Open deal count × list price × stage probability (10% Prospecting, 40% Engaging) |
| **Lost** | `deal_stage = 'Lost'` — rep explicitly marked it closed-lost |
| **Stalled** | `deal_stage = 'Engaging'` with no `close_date` — open, no outcome yet |
| **Never Engaged** | `engage_date IS NULL` — deal never advanced past Prospecting |

---

## Dataset

**Source:** Maven Analytics CRM + Sales Opportunities dataset

| Table | Rows | Description |
|---|---|---|
| `sales_pipeline.csv` | 8,800 | One row per opportunity |
| `accounts.csv` | 85 | Account/company metadata |
| `products.csv` | 7 | Product catalog with list price |
| `sales_teams.csv` | 30 | Rep → Manager → Region mapping |

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Google BigQuery |
| Transformation | SQL (CTEs, Window Functions, Date Functions) |
| Visualization | Looker Studio |
| Reporting | PDF Executive Summary (ReportLab) |
| Version Control | GitHub |

---

## SQL Scripts

All scripts are in the `/sql` directory and intended to run sequentially in BigQuery.

| Script | Purpose |
|---|---|
| `01_etl_cleaning.sql` | Upload CSVs as raw tables; clean nulls with `COALESCE`/`CASE`; flag Lost vs Stalled deals; cast date types; compute `days_to_close` and `quarter_opened` |
| `02_star_schema.sql` | Build `fact_opportunities` joined to `dim_agent`, `dim_product`, `dim_account`, `dim_date`; add `stage_probability` and `weighted_pipeline_value` columns |
| `03_funnel_analysis.sql` | CTE-based funnel persistence; leakage by product and region; cohort heatmap data (Month Opened × Win Rate) |
| `04_rep_performance.sql` | Full rep scorecard with `PERCENT_RANK()`; top/bottom 10% identification; revenue lift model for underperformers |
| `05_pipeline_health.sql` | Open pipeline weighted by probability; quarterly revenue trends; product revenue breakdown; executive KPI summary |

### Key SQL Patterns Used

**Null `engage_date` handling** — deals that died at Prospecting are flagged but not dropped:
```sql
COALESCE(CAST(engage_date AS DATE), NULL) AS engage_date,
CASE WHEN engage_date IS NULL THEN TRUE ELSE FALSE END AS never_engaged
```

**Feature engineering:**
```sql
DATE_DIFF(close_date, engage_date, DAY) AS days_to_close,
EXTRACT(QUARTER FROM COALESCE(engage_date, close_date)) AS quarter_opened
```

**CTE funnel persistence:**
```sql
WITH stage_counts AS (
    SELECT
        COUNT(DISTINCT opportunity_id)                              AS total_prospecting,
        COUNT(DISTINCT IF(NOT never_engaged, opportunity_id, NULL)) AS reached_engaging,
        COUNT(DISTINCT IF(is_won, opportunity_id, NULL))            AS won
    FROM analytics.fact_opportunities
)
SELECT *, ROUND(won / reached_engaging * 100, 1) AS engaging_to_won_rate
FROM stage_counts;
```

**Lost vs Stalled definition:**
- **Lost** = `deal_stage = 'Lost'` (rep explicitly closed the deal as lost)
- **Stalled** = `deal_stage = 'Engaging'` AND `close_date IS NULL` (still open, no outcome)

---

## Looker Studio Dashboard

Looker Studio connects directly to `analytics.fact_opportunities` (joined to dimensions via BigQuery views). The dashboard includes five panels:

1. **Revenue Trend** — Monthly time-series of `close_value` filtered by product and region
2. **Deal Cohort Heatmap** — Pivot table: Month Opened (rows) × Win Rate (values), colored by performance vs. average
3. **Rep Performance Table** — Win Rate, Avg Cycle, and Pipeline Value per rep with conditional formatting
4. **Pipeline Health Bar Chart** — Open deals by stage, bars weighted by `weighted_pipeline_value`
5. **KPI Scorecards** — Win rate, total revenue, avg ACV, avg cycle days

**Calculated Fields in Looker Studio:**
```
Win Rate = COUNT(CASE WHEN deal_stage='Won' THEN opportunity_id END) / COUNT(opportunity_id)
Loss Rate = COUNT(CASE WHEN deal_stage='Lost' THEN opportunity_id END) /
            COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN opportunity_id END)
Weighted Pipeline = SUM(weighted_pipeline_value)
```

---

## Key Findings

| Finding | Detail |
|---|---|
| Overall win rate | **63.2%** of closed deals; 48.2% of all 8,800 opportunities |
| Leakiest stage | **Engaging → Lost**: 2,473 deals lost after engaging (29.8% of engaged pipeline) |
| Best product by win rate | MG Special: **64.8%** (but low ACV of $55) |
| Worst product by win rate | MG Advanced: **60.3%** (high ACV of $3,389 — real money at risk) |
| Top rep | Hayden Neloms: **70.4%** win rate |
| Bottom rep | Lajuana Vencill: **55.0%** win rate + longest cycle (63 days) |
| Avg sales cycle | **51.8 days** (Won deals) |
| Total won revenue | **$10,005,534** |
| Est. lost revenue opportunity | ~**$5.85M** (lost deals × avg ACV by product) |

---

## Repository Structure

```
b2b-sales-pipeline-analysis/
├── CRM+Sales+Opportunities/     # Raw source CSVs
│   ├── sales_pipeline.csv
│   ├── accounts.csv
│   ├── products.csv
│   ├── sales_teams.csv
│   └── data_dictionary.csv
├── sql/
│   ├── 01_etl_cleaning.sql
│   ├── 02_star_schema.sql
│   ├── 03_funnel_analysis.sql
│   ├── 04_rep_performance.sql
│   └── 05_pipeline_health.sql
├── Project_Insights_VP.pdf      # Executive memo for VP of Sales
└── README.md
```

---
