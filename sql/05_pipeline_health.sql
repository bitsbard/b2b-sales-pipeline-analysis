-- ============================================================
-- 05_pipeline_health.sql
-- MavenTech B2B Sales Pipeline Analysis
-- Phase 4: Open Pipeline Health & Revenue Forecasting
--
-- Powers the "Pipeline Health" bar chart in Looker Studio.
-- Also provides quarterly trend data and product revenue views.
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. OPEN PIPELINE BY STAGE (weighted by close probability)
--    Used for the Looker Studio "Pipeline Health" bar chart
-- ─────────────────────────────────────────────────────────────
SELECT
    f.deal_stage,
    dp.product,
    dp.series,
    da.regional_office,
    da.manager,
    COUNT(DISTINCT f.opportunity_id)            AS open_deal_count,
    ROUND(SUM(dp.list_price), 0)                AS gross_pipeline_value,
    ROUND(SUM(f.weighted_pipeline_value), 0)    AS weighted_pipeline_value,
    ROUND(AVG(f.stage_probability) * 100, 0)    AS stage_probability_pct
FROM analytics.fact_opportunities f
JOIN analytics.dim_agent   da ON f.agent_key   = da.agent_key
JOIN analytics.dim_product dp ON f.product_key = dp.product_key
WHERE f.deal_stage IN ('Prospecting', 'Engaging')   -- open deals only
GROUP BY
    f.deal_stage,
    dp.product,
    dp.series,
    da.regional_office,
    da.manager
ORDER BY f.deal_stage, weighted_pipeline_value DESC;


-- ─────────────────────────────────────────────────────────────
-- 2. QUARTERLY REVENUE TREND (Won deals)
--    Powers the time-series chart in Looker Studio
-- ─────────────────────────────────────────────────────────────
SELECT
    dd.quarter_label,
    dd.year,
    dd.quarter,
    dp.series,
    da.regional_office,
    COUNT(DISTINCT f.opportunity_id)            AS won_deals,
    ROUND(SUM(f.close_value), 0)                AS won_revenue,
    ROUND(AVG(f.close_value), 0)                AS avg_deal_size,
    ROUND(AVG(f.days_to_close), 1)              AS avg_cycle_days
FROM analytics.fact_opportunities f
JOIN analytics.dim_date    dd ON f.date_key    = dd.date_key
JOIN analytics.dim_product dp ON f.product_key = dp.product_key
JOIN analytics.dim_agent   da ON f.agent_key   = da.agent_key
WHERE f.is_won = TRUE
GROUP BY
    dd.quarter_label,
    dd.year,
    dd.quarter,
    dp.series,
    da.regional_office
ORDER BY dd.year, dd.quarter;


-- ─────────────────────────────────────────────────────────────
-- 3. PRODUCT REVENUE BREAKDOWN (for Looker Studio scorecards)
-- ─────────────────────────────────────────────────────────────
SELECT
    dp.product,
    dp.series,
    dp.list_price,
    COUNT(DISTINCT f.opportunity_id)                              AS total_deals,
    COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))         AS won,
    COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))         AS lost,
    ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)                 AS total_revenue,
    ROUND(AVG(IF(f.is_won, f.close_value, NULL)), 0)              AS avg_acv,
    ROUND(
        COUNT(DISTINCT IF(f.is_won, f.opportunity_id, NULL))
        / NULLIF(
            COUNT(DISTINCT IF(f.is_won OR f.is_lost, f.opportunity_id, NULL))
          , 0) * 100
    , 1)                                                          AS win_rate_pct,
    -- Discount vs list price
    ROUND(
        (dp.list_price - AVG(IF(f.is_won, f.close_value, NULL)))
        / NULLIF(dp.list_price, 0) * 100
    , 1)                                                          AS avg_discount_pct
FROM analytics.fact_opportunities f
JOIN analytics.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.product, dp.series, dp.list_price
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────────
-- 4. LOOKER STUDIO CALCULATED FIELD REFERENCE
--    These formulas should be entered as Calculated Fields
--    directly in Looker Studio after connecting to BigQuery.
--
--    Win Rate:
--      COUNT(CASE WHEN deal_stage = 'Won' THEN opportunity_id ELSE NULL END)
--      / COUNT(opportunity_id)
--
--    Weighted Pipeline:
--      SUM(weighted_pipeline_value)
--
--    Avg Sales Cycle (Won):
--      AVG(CASE WHEN is_won THEN days_to_close ELSE NULL END)
--
--    Loss Rate:
--      COUNT(CASE WHEN deal_stage = 'Lost' THEN opportunity_id ELSE NULL END)
--      / COUNT(CASE WHEN deal_stage IN ('Won','Lost') THEN opportunity_id ELSE NULL END)
-- ─────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────
-- 5. EXECUTIVE KPI SUMMARY (for scorecard tiles)
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT opportunity_id)                                  AS total_opportunities,
    COUNT(DISTINCT IF(is_won,  opportunity_id, NULL))               AS total_won,
    COUNT(DISTINCT IF(is_lost, opportunity_id, NULL))               AS total_lost,
    COUNT(DISTINCT IF(is_stalled OR never_engaged, opportunity_id, NULL)) AS open_pipeline,
    ROUND(SUM(IF(is_won, close_value, 0)), 0)                       AS total_won_revenue,
    ROUND(AVG(IF(is_won, close_value, NULL)), 0)                    AS avg_acv,
    ROUND(
        COUNT(DISTINCT IF(is_won, opportunity_id, NULL))
        / NULLIF(
            COUNT(DISTINCT IF(is_won OR is_lost, opportunity_id, NULL))
          , 0) * 100
    , 1)                                                            AS overall_win_rate_pct,
    ROUND(AVG(IF(is_won, days_to_close, NULL)), 1)                  AS avg_sales_cycle_days,
    ROUND(SUM(weighted_pipeline_value), 0)                          AS total_weighted_pipeline
FROM analytics.fact_opportunities;
