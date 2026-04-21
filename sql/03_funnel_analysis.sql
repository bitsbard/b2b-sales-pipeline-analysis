-- ============================================================
-- 03_funnel_analysis.sql
-- MavenTech B2B Sales Pipeline Analysis
-- Phase 3: Funnel & Leakage Analysis (CTE-based)
--
-- Key definitions used throughout:
--   LOST deal   = deal_stage = 'Lost' (explicitly marked by rep)
--   STALLED deal = deal_stage = 'Engaging' with no close_date (open, no outcome yet)
--   NEVER ENGAGED = engage_date IS NULL AND deal_stage = 'Prospecting' (died in cold storage)
--
-- Funnel stages:
--   Stage 1 – Prospecting : All deals enter here
--   Stage 2 – Engaging    : Deals where engage_date IS NOT NULL
--   Stage 3 – Closed      : deal_stage IN ('Won', 'Lost')
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. OVERALL FUNNEL PERSISTENCE
-- ─────────────────────────────────────────────────────────────
WITH stage_counts AS (
    SELECT
        COUNT(DISTINCT opportunity_id)                                    AS total_prospecting,
        COUNT(DISTINCT IF(NOT never_engaged, opportunity_id, NULL))       AS reached_engaging,
        COUNT(DISTINCT IF(is_won OR is_lost,  opportunity_id, NULL))      AS reached_closed,
        COUNT(DISTINCT IF(is_won,             opportunity_id, NULL))      AS won,
        COUNT(DISTINCT IF(is_lost,            opportunity_id, NULL))      AS lost,
        COUNT(DISTINCT IF(is_stalled,         opportunity_id, NULL))      AS stalled_open,
        COUNT(DISTINCT IF(never_engaged,      opportunity_id, NULL))      AS never_engaged
    FROM analytics.fact_opportunities
)

SELECT
    total_prospecting,
    reached_engaging,
    reached_closed,
    won,
    lost,
    stalled_open,
    never_engaged,

    -- Conversion rates
    ROUND(reached_engaging / total_prospecting * 100, 1)  AS pct_prospecting_to_engaging,
    ROUND(reached_closed   / reached_engaging  * 100, 1)  AS pct_engaging_to_closed,
    ROUND(won              / reached_engaging  * 100, 1)  AS pct_engaging_to_won,
    ROUND(lost             / reached_engaging  * 100, 1)  AS pct_engaging_to_lost,
    ROUND(won              / reached_closed    * 100, 1)  AS overall_win_rate,

    -- "Leaky stage" — highest absolute deal count that dies
    CASE
        WHEN never_engaged > lost THEN 'Prospecting (never advanced to Engaging)'
        ELSE 'Engaging (advanced but ultimately Lost)'
    END AS leakiest_stage

FROM stage_counts;


-- ─────────────────────────────────────────────────────────────
-- 2. FUNNEL LEAKAGE BY PRODUCT
--    Identifies which product line has the worst Engaging → Won rate
-- ─────────────────────────────────────────────────────────────
WITH product_funnel AS (
    SELECT
        dp.product,
        dp.series,
        dp.list_price,
        COUNT(DISTINCT f.opportunity_id)                                AS total_deals,
        COUNT(DISTINCT IF(NOT f.never_engaged, f.opportunity_id, NULL)) AS reached_engaging,
        COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))           AS won,
        COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))           AS lost,
        ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)                   AS actual_won_revenue,
        -- Opportunity cost: lost deals × avg close value for that product
        ROUND(
            COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))
            * AVG(IF(f.is_won, f.close_value, NULL))
        , 0)                                                            AS est_lost_revenue
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_product         dp ON f.product_key = dp.product_key
    GROUP BY dp.product, dp.series, dp.list_price
)

SELECT
    product,
    series,
    list_price,
    total_deals,
    reached_engaging,
    won,
    lost,
    ROUND(won  / NULLIF(won + lost, 0) * 100, 1) AS win_rate_pct,
    ROUND(lost / NULLIF(won + lost, 0) * 100, 1) AS loss_rate_pct,
    actual_won_revenue,
    est_lost_revenue,
    RANK() OVER (ORDER BY lost / NULLIF(won + lost, 0) DESC) AS loss_rank  -- 1 = worst
FROM product_funnel
ORDER BY loss_rank;


-- ─────────────────────────────────────────────────────────────
-- 3. FUNNEL LEAKAGE BY REGION
-- ─────────────────────────────────────────────────────────────
WITH region_funnel AS (
    SELECT
        da.regional_office,
        COUNT(DISTINCT f.opportunity_id)                                AS total_deals,
        COUNT(DISTINCT IF(f.is_won OR f.is_lost, f.opportunity_id, NULL)) AS closed_deals,
        COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))           AS won,
        COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))           AS lost,
        ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)                   AS won_revenue
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_agent           da ON f.agent_key = da.agent_key
    GROUP BY da.regional_office
)

SELECT
    regional_office,
    total_deals,
    closed_deals,
    won,
    lost,
    ROUND(won / NULLIF(closed_deals, 0) * 100, 1) AS win_rate_pct,
    won_revenue,
    RANK() OVER (ORDER BY won / NULLIF(closed_deals, 0) DESC) AS win_rank
FROM region_funnel
ORDER BY win_rank;


-- ─────────────────────────────────────────────────────────────
-- 4. DEAL COHORT HEATMAP DATA
--    "Month Opened" vs Win Rate — for Looker Studio pivot table
-- ─────────────────────────────────────────────────────────────
SELECT
    FORMAT_DATE('%Y-%m', f.engage_date)    AS month_opened,
    COUNT(DISTINCT f.opportunity_id)       AS total_deals,
    COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL)) AS won,
    COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL)) AS lost,
    ROUND(
        COUNT(DISTINCT IF(f.is_won, f.opportunity_id, NULL))
        / NULLIF(
            COUNT(DISTINCT IF(f.is_won OR f.is_lost, f.opportunity_id, NULL))
          , 0) * 100
    , 1)                                   AS win_rate_pct
FROM analytics.fact_opportunities f
WHERE f.engage_date IS NOT NULL
GROUP BY month_opened
ORDER BY month_opened;


-- ─────────────────────────────────────────────────────────────
-- 5. GLOBALLY WEAKEST STAGE — SUMMARY OUTPUT
--    For README and executive memo reference
-- ─────────────────────────────────────────────────────────────
WITH summary AS (
    SELECT
        'Prospecting → Engaging' AS transition,
        500                      AS deals_dropped,
        5.7                      AS drop_off_pct,
        0                        AS est_revenue_impact
    UNION ALL
    SELECT
        'Engaging → Won (Lost deals)',
        2473,
        29.8,
        5854000   -- estimated from avg ACV × lost count
)
SELECT * FROM summary ORDER BY drop_off_pct DESC;
