-- ============================================================
-- MavenTech Sales Pipeline — SQLite Star Schema
-- Optional pre-processing step: run before loading into Power BI
-- to keep SQL visible alongside the DAX/M work.
--
-- Usage:
--   sqlite3 maventech.db < star_schema.sql
-- ============================================================

PRAGMA foreign_keys = ON;

-- ────────────────────────────────────────────────────────────
-- STEP 1: Load raw CSVs into staging tables
-- (Using SQLite's .import via shell, or attach via Python)
-- ────────────────────────────────────────────────────────────

-- Staging: raw pipeline
CREATE TABLE IF NOT EXISTS stg_pipeline (
    opportunity_id TEXT,
    sales_agent    TEXT,
    product        TEXT,
    account        TEXT,
    deal_stage     TEXT,
    engage_date    TEXT,   -- stored as ISO string; cast on transform
    close_date     TEXT,
    close_value    TEXT    -- nullable; cast to REAL on transform
);

-- Staging: accounts
CREATE TABLE IF NOT EXISTS stg_accounts (
    account          TEXT,
    sector           TEXT,
    year_established TEXT,
    revenue          TEXT,
    employees        TEXT,
    office_location  TEXT,
    subsidiary_of    TEXT
);

-- Staging: products
CREATE TABLE IF NOT EXISTS stg_products (
    product     TEXT,
    series      TEXT,
    sales_price TEXT
);

-- Staging: teams
CREATE TABLE IF NOT EXISTS stg_teams (
    sales_agent     TEXT,
    manager         TEXT,
    regional_office TEXT
);


-- ────────────────────────────────────────────────────────────
-- STEP 2: Dimension tables (cleaned)
-- ────────────────────────────────────────────────────────────

-- dim_product
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product AS
SELECT
    product,
    COALESCE(series, 'Unknown') AS series,
    CAST(sales_price AS REAL)   AS sales_price
FROM stg_products
WHERE product IS NOT NULL;

-- dim_account
-- Cleans the "technolgy" typo in sector
DROP TABLE IF EXISTS dim_account;
CREATE TABLE dim_account AS
SELECT
    account,
    CASE WHEN LOWER(sector) = 'technolgy' THEN 'technology' ELSE sector END AS sector,
    CAST(year_established AS INTEGER) AS year_established,
    CAST(revenue          AS REAL)    AS revenue_millions,
    CAST(employees        AS INTEGER) AS employees,
    office_location,
    NULLIF(subsidiary_of, '')         AS subsidiary_of
FROM stg_accounts;

-- dim_agent
DROP TABLE IF EXISTS dim_agent;
CREATE TABLE dim_agent AS
SELECT
    sales_agent,
    manager,
    regional_office
FROM stg_teams;

-- dim_date  (generated via recursive CTE — no external data needed)
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date AS
WITH RECURSIVE dates(d) AS (
    SELECT '2016-10-01'
    UNION ALL
    SELECT DATE(d, '+1 day')
    FROM dates
    WHERE d < '2018-12-31'
)
SELECT
    d                                                                  AS date,
    CAST(STRFTIME('%Y', d) AS INTEGER)                                 AS year,
    CAST(STRFTIME('%m', d) AS INTEGER)                                 AS month_num,
    STRFTIME('%Y-%m', d)                                               AS year_month,
    CAST(CEIL(CAST(STRFTIME('%m', d) AS REAL) / 3.0) AS INTEGER)      AS quarter_num,
    STRFTIME('%Y', d) || '-Q' ||
        CAST(CEIL(CAST(STRFTIME('%m', d) AS REAL) / 3.0) AS TEXT)     AS quarter_label,
    CASE WHEN CAST(STRFTIME('%w', d) AS INTEGER) IN (0, 6)
         THEN 1 ELSE 0 END                                             AS is_weekend,
    -- Fiscal year: Oct 1 = FQ1
    CASE
        WHEN CAST(STRFTIME('%m', d) AS INTEGER) >= 10
        THEN CAST(STRFTIME('%Y', d) AS INTEGER) + 1
        ELSE CAST(STRFTIME('%Y', d) AS INTEGER)
    END                                                                AS fiscal_year,
    CASE
        WHEN CAST(STRFTIME('%m', d) AS INTEGER) >= 10 THEN 1
        WHEN CAST(STRFTIME('%m', d) AS INTEGER) >= 7  THEN 4
        WHEN CAST(STRFTIME('%m', d) AS INTEGER) >= 4  THEN 3
        ELSE 2
    END                                                                AS fiscal_quarter
FROM dates;

CREATE INDEX IF NOT EXISTS idx_dim_date ON dim_date(date);


-- ────────────────────────────────────────────────────────────
-- STEP 3: Fact table (cleaned, enriched)
-- ────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS fact_opportunities;
CREATE TABLE fact_opportunities AS
SELECT
    p.opportunity_id,
    p.sales_agent,
    p.product,
    p.account,
    p.deal_stage,

    -- Boolean flags
    CASE WHEN p.engage_date IS NOT NULL AND p.engage_date != '' THEN 1 ELSE 0 END
        AS reached_engaging,
    CASE WHEN p.deal_stage IN ('Won', 'Lost') THEN 1 ELSE 0 END
        AS is_closed,
    CASE WHEN p.deal_stage = 'Won' THEN 1 ELSE 0 END
        AS is_won,

    -- Dates (NULL if missing/empty)
    NULLIF(p.engage_date, '')  AS engage_date,
    NULLIF(p.close_date,  '')  AS close_date,

    -- Derived: sales cycle length (days)
    -- Only defined for closed deals with a valid engage_date
    CASE
        WHEN p.deal_stage IN ('Won', 'Lost')
         AND p.engage_date IS NOT NULL AND p.engage_date != ''
         AND p.close_date  IS NOT NULL AND p.close_date  != ''
        THEN CAST(JULIANDAY(p.close_date) - JULIANDAY(p.engage_date) AS INTEGER)
        ELSE NULL
    END AS days_to_close,

    -- Derived: quarter and month of engagement (for cohort analysis)
    CASE
        WHEN p.engage_date IS NOT NULL AND p.engage_date != ''
        THEN STRFTIME('%Y', p.engage_date) || '-Q' ||
             CAST(CEIL(CAST(STRFTIME('%m', p.engage_date) AS REAL) / 3.0) AS TEXT)
        ELSE NULL
    END AS quarter_opened,

    CASE
        WHEN p.engage_date IS NOT NULL AND p.engage_date != ''
        THEN STRFTIME('%Y-%m', p.engage_date) || '-01'
        ELSE NULL
    END AS month_opened,

    -- Derived: quarter closed (for revenue trending)
    CASE
        WHEN p.close_date IS NOT NULL AND p.close_date != ''
        THEN STRFTIME('%Y', p.close_date) || '-Q' ||
             CAST(CEIL(CAST(STRFTIME('%m', p.close_date) AS REAL) / 3.0) AS TEXT)
        ELSE NULL
    END AS quarter_closed,

    -- Revenue: NULL for open/lost deals (correct — do not impute)
    CAST(NULLIF(p.close_value, '') AS REAL) AS close_value

FROM stg_pipeline p;

-- Indexes for common filter patterns
CREATE INDEX IF NOT EXISTS idx_fact_stage   ON fact_opportunities(deal_stage);
CREATE INDEX IF NOT EXISTS idx_fact_agent   ON fact_opportunities(sales_agent);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_opportunities(product);
CREATE INDEX IF NOT EXISTS idx_fact_qopen   ON fact_opportunities(quarter_opened);
CREATE INDEX IF NOT EXISTS idx_fact_qclose  ON fact_opportunities(quarter_closed);


-- ────────────────────────────────────────────────────────────
-- STEP 4: Validation queries (run after load to QA the model)
-- ────────────────────────────────────────────────────────────

-- Row count check
SELECT 'fact_opportunities' AS tbl, COUNT(*) AS rows FROM fact_opportunities
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_account',  COUNT(*) FROM dim_account
UNION ALL SELECT 'dim_agent',    COUNT(*) FROM dim_agent;

-- Funnel check
SELECT
    deal_stage,
    COUNT(*)                                     AS n,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM fact_opportunities
GROUP BY deal_stage
ORDER BY n DESC;

-- Revenue sanity
SELECT
    SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END) AS total_won_revenue,
    AVG(CASE WHEN deal_stage = 'Won' THEN close_value END)        AS avg_deal_size,
    AVG(CASE WHEN deal_stage = 'Won' THEN days_to_close END)      AS avg_cycle_days
FROM fact_opportunities;

-- Win rate by product
SELECT
    product,
    COUNT(*)                                           AS closed_deals,
    SUM(is_won)                                        AS won,
    ROUND(SUM(is_won) * 100.0 / COUNT(*), 1)          AS win_rate_pct,
    ROUND(AVG(CASE WHEN is_won = 1 THEN close_value END), 0) AS avg_acv
FROM fact_opportunities
WHERE is_closed = 1
GROUP BY product
ORDER BY win_rate_pct ASC;

-- Win rate by region (via join to dim_agent)
SELECT
    a.regional_office,
    COUNT(*)                                      AS closed_deals,
    SUM(f.is_won)                                 AS won,
    ROUND(SUM(f.is_won) * 100.0 / COUNT(*), 1)   AS win_rate_pct,
    ROUND(SUM(f.close_value), 0)                  AS total_revenue
FROM fact_opportunities f
JOIN dim_agent a ON f.sales_agent = a.sales_agent
WHERE f.is_closed = 1
GROUP BY a.regional_office
ORDER BY win_rate_pct ASC;

-- Rep performance (for coaching identification)
SELECT
    f.sales_agent,
    a.manager,
    a.regional_office,
    COUNT(*)                                              AS closed,
    SUM(f.is_won)                                         AS won,
    ROUND(SUM(f.is_won) * 100.0 / COUNT(*), 1)           AS win_rate_pct,
    ROUND(AVG(CASE WHEN f.is_won = 1 THEN f.days_to_close END), 1) AS avg_cycle
FROM fact_opportunities f
JOIN dim_agent a ON f.sales_agent = a.sales_agent
WHERE f.is_closed = 1
GROUP BY f.sales_agent
ORDER BY win_rate_pct ASC;
