-- ============================================================
-- 02_star_schema.sql
-- MavenTech B2B Sales Pipeline Analysis
-- Phase 2: Star Schema — Fact + Dimension Tables
--
-- Schema: analytics
-- Fact table  : fact_opportunities
-- Dimensions  : dim_agent, dim_product, dim_account, dim_date
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- DIM: Agent (sales rep + manager + region)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE analytics.dim_agent AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.sales_agent)  AS agent_key,
    t.sales_agent,
    t.manager,
    t.regional_office
FROM staging.stg_sales_teams t;


-- ─────────────────────────────────────────────────────────────
-- DIM: Product (with list price and series)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE analytics.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product)      AS product_key,
    p.product,
    p.series,
    p.list_price
FROM staging.stg_products p;


-- ─────────────────────────────────────────────────────────────
-- DIM: Account (company metadata)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE analytics.dim_account AS
SELECT
    ROW_NUMBER() OVER (ORDER BY a.account)      AS account_key,
    a.account,
    a.sector,
    a.year_established,
    a.revenue_m,
    a.employees,
    a.office_location,
    a.subsidiary_of
FROM staging.stg_accounts a;


-- ─────────────────────────────────────────────────────────────
-- DIM: Date (spine covering full dataset range 2016-10 → 2017-12)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE analytics.dim_date AS
SELECT
    FORMAT_DATE('%Y%m%d', d)         AS date_key,
    d                                AS full_date,
    EXTRACT(YEAR    FROM d)          AS year,
    EXTRACT(QUARTER FROM d)          AS quarter,
    EXTRACT(MONTH   FROM d)          AS month,
    FORMAT_DATE('%b %Y', d)          AS month_label,
    CONCAT('Q', EXTRACT(QUARTER FROM d), ' ', EXTRACT(YEAR FROM d)) AS quarter_label
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2016-10-01', DATE '2017-12-31', INTERVAL 1 DAY)
) AS d;


-- ─────────────────────────────────────────────────────────────
-- FACT: Opportunities
-- Central grain = one row per opportunity
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE analytics.fact_opportunities AS
SELECT
    p.opportunity_id,

    -- Foreign keys to dimensions
    a.agent_key,
    pr.product_key,
    ac.account_key,
    FORMAT_DATE('%Y%m%d', COALESCE(p.engage_date, p.close_date)) AS date_key,

    -- Raw dates for time-series
    p.engage_date,
    p.close_date,

    -- Stage & classification
    p.deal_stage,
    p.deal_status,
    p.is_won,
    p.is_lost,
    p.is_stalled,
    p.never_engaged,

    -- Measures
    p.close_value,
    p.days_to_close,
    p.quarter_opened,
    p.year_opened,

    -- Probability weight for pipeline forecasting
    CASE p.deal_stage
        WHEN 'Prospecting' THEN 0.10
        WHEN 'Engaging'    THEN 0.40
        WHEN 'Won'         THEN 1.00
        WHEN 'Lost'        THEN 0.00
        ELSE 0.00
    END                                                           AS stage_probability,

    -- Weighted pipeline value (for open deals only)
    CASE
        WHEN p.deal_stage IN ('Prospecting', 'Engaging')
        THEN pr_dim.list_price * (
            CASE p.deal_stage
                WHEN 'Prospecting' THEN 0.10
                WHEN 'Engaging'    THEN 0.40
            END)
        ELSE 0
    END                                                           AS weighted_pipeline_value

FROM staging.stg_sales_pipeline         p
LEFT JOIN analytics.dim_agent            a  ON p.sales_agent = a.sales_agent
LEFT JOIN analytics.dim_product          pr ON p.product      = pr.product
LEFT JOIN analytics.dim_account          ac ON p.account       = ac.account
LEFT JOIN analytics.dim_product          pr_dim ON p.product   = pr_dim.product;


-- ─────────────────────────────────────────────────────────────
-- QUICK SANITY CHECK
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                           AS total_opportunities,
    COUNTIF(is_won)                                    AS won,
    COUNTIF(is_lost)                                   AS lost,
    COUNTIF(is_stalled)                                AS stalled,
    COUNTIF(never_engaged)                             AS never_engaged,
    ROUND(SUM(IF(is_won, close_value, 0)), 0)          AS total_won_revenue,
    ROUND(AVG(IF(is_won, days_to_close, NULL)), 1)     AS avg_days_to_close_won
FROM analytics.fact_opportunities;
