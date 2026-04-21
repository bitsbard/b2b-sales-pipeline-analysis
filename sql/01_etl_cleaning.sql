-- ============================================================
-- 01_etl_cleaning.sql
-- MavenTech B2B Sales Pipeline Analysis
-- Phase 2: ETL & Data Cleaning in BigQuery
--
-- Assumptions:
--   Raw CSVs have been uploaded to BigQuery as:
--     raw.sales_pipeline, raw.accounts, raw.products, raw.sales_teams
--
-- This script creates clean staging tables with null handling,
-- type casts, and basic validation flags.
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- STAGING: sales_pipeline (fact source)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE staging.stg_sales_pipeline AS
SELECT
    opportunity_id,
    TRIM(sales_agent)                                     AS sales_agent,
    TRIM(product)                                         AS product,
    TRIM(account)                                         AS account,
    TRIM(deal_stage)                                      AS deal_stage,

    -- engage_date is NULL for deals still at Prospecting.
    -- COALESCE preserves NULL — we flag these explicitly below.
    CAST(engage_date AS DATE)                             AS engage_date,
    CAST(close_date  AS DATE)                             AS close_date,

    -- close_value is 0 / NULL for Lost & open deals
    COALESCE(CAST(close_value AS FLOAT64), 0.0)           AS close_value,

    -- Derived: days from first engagement to close
    DATE_DIFF(
        CAST(close_date  AS DATE),
        CAST(engage_date AS DATE),
        DAY
    )                                                     AS days_to_close,

    -- Quarter the deal was opened (using engage_date as proxy for create_date)
    EXTRACT(QUARTER FROM CAST(COALESCE(engage_date, close_date) AS DATE))
                                                          AS quarter_opened,
    EXTRACT(YEAR   FROM CAST(COALESCE(engage_date, close_date) AS DATE))
                                                          AS year_opened,

    -- Deal classification flags
    CASE
        WHEN deal_stage = 'Won'  THEN 'Closed Won'
        WHEN deal_stage = 'Lost' THEN 'Closed Lost'
        WHEN deal_stage = 'Engaging' AND close_date IS NULL THEN 'Open – Engaging'
        WHEN deal_stage = 'Prospecting' AND engage_date IS NULL THEN 'Open – Prospecting'
        ELSE 'Unknown'
    END                                                   AS deal_status,

    -- A "Stalled" deal = still Engaging with no close date (open pipeline)
    -- A "Lost" deal    = explicitly marked Lost in deal_stage
    CASE
        WHEN deal_stage = 'Lost'     THEN TRUE ELSE FALSE
    END                                                   AS is_lost,

    CASE
        WHEN deal_stage = 'Engaging' AND close_date IS NULL THEN TRUE ELSE FALSE
    END                                                   AS is_stalled,

    CASE
        WHEN deal_stage = 'Won'      THEN TRUE ELSE FALSE
    END                                                   AS is_won,

    CASE
        WHEN engage_date IS NULL     THEN TRUE ELSE FALSE
    END                                                   AS never_engaged  -- died at Prospecting

FROM raw.sales_pipeline;


-- ─────────────────────────────────────────────────────────────
-- STAGING: accounts
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE staging.stg_accounts AS
SELECT
    TRIM(account)                                         AS account,
    TRIM(sector)                                          AS sector,
    CAST(year_established AS INT64)                       AS year_established,
    CAST(revenue AS FLOAT64)                              AS revenue_m,      -- millions USD
    CAST(employees AS INT64)                              AS employees,
    TRIM(office_location)                                 AS office_location,
    COALESCE(TRIM(subsidiary_of), 'Independent')          AS subsidiary_of
FROM raw.accounts;


-- ─────────────────────────────────────────────────────────────
-- STAGING: products
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE staging.stg_products AS
SELECT
    TRIM(product)                                         AS product,
    TRIM(series)                                          AS series,
    CAST(sales_price AS FLOAT64)                          AS list_price
FROM raw.products;


-- ─────────────────────────────────────────────────────────────
-- STAGING: sales_teams
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE staging.stg_sales_teams AS
SELECT
    TRIM(sales_agent)                                     AS sales_agent,
    TRIM(manager)                                         AS manager,
    TRIM(regional_office)                                 AS regional_office
FROM raw.sales_teams;


-- ─────────────────────────────────────────────────────────────
-- VALIDATION CHECKS (run after staging to confirm data quality)
-- ─────────────────────────────────────────────────────────────

-- Check 1: Row count expectations
SELECT 'stg_sales_pipeline' AS tbl, COUNT(*) AS rows FROM staging.stg_sales_pipeline
UNION ALL
SELECT 'stg_accounts',      COUNT(*) FROM staging.stg_accounts
UNION ALL
SELECT 'stg_products',      COUNT(*) FROM staging.stg_products
UNION ALL
SELECT 'stg_sales_teams',   COUNT(*) FROM staging.stg_sales_teams;

-- Check 2: Null engage_date summary (deals that never left Prospecting)
SELECT
    never_engaged,
    COUNT(*) AS deal_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM staging.stg_sales_pipeline
GROUP BY never_engaged;

-- Check 3: Stage distribution
SELECT deal_stage, COUNT(*) AS cnt
FROM staging.stg_sales_pipeline
GROUP BY deal_stage
ORDER BY cnt DESC;
