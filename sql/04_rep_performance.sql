-- ============================================================
-- 04_rep_performance.sql
-- MavenTech B2B Sales Pipeline Analysis
-- Phase 3 / Phase 4: Sales Rep Benchmarking
--
-- Produces the Rep Performance table for the Looker Studio
-- dashboard and identifies top/bottom decile performers.
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. FULL REP SCORECARD
-- ─────────────────────────────────────────────────────────────
WITH rep_base AS (
    SELECT
        da.sales_agent,
        da.manager,
        da.regional_office,
        COUNT(DISTINCT f.opportunity_id)                                  AS total_deals,
        COUNT(DISTINCT IF(f.is_won,    f.opportunity_id, NULL))           AS won,
        COUNT(DISTINCT IF(f.is_lost,   f.opportunity_id, NULL))           AS lost,
        COUNT(DISTINCT IF(f.is_stalled,f.opportunity_id, NULL))           AS stalled,
        ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)                     AS total_revenue,
        ROUND(AVG(IF(f.is_won, f.close_value, NULL)), 0)                  AS avg_acv,
        ROUND(AVG(IF(f.is_won, f.days_to_close, NULL)), 1)                AS avg_days_to_close,
        ROUND(SUM(f.weighted_pipeline_value), 0)                          AS open_pipeline_value
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_agent           da ON f.agent_key = da.agent_key
    GROUP BY da.sales_agent, da.manager, da.regional_office
),

rep_with_rates AS (
    SELECT
        *,
        ROUND(won / NULLIF(won + lost, 0) * 100, 1)  AS win_rate_pct,
        -- Percentile rank for win rate across all reps (1 = best)
        PERCENT_RANK() OVER (ORDER BY won / NULLIF(won + lost, 0) DESC) AS win_rate_prank
    FROM rep_base
)

SELECT
    sales_agent,
    manager,
    regional_office,
    total_deals,
    won,
    lost,
    stalled,
    win_rate_pct,
    total_revenue,
    avg_acv,
    avg_days_to_close,
    open_pipeline_value,
    win_rate_prank,
    CASE
        WHEN win_rate_prank <= 0.10 THEN 'Top 10%'
        WHEN win_rate_prank >= 0.90 THEN 'Bottom 10%'
        ELSE 'Core'
    END AS performance_tier
FROM rep_with_rates
ORDER BY win_rate_pct DESC;


-- ─────────────────────────────────────────────────────────────
-- 2. TOP 10% PERFORMERS
-- ─────────────────────────────────────────────────────────────
WITH rep_base AS (
    SELECT
        da.sales_agent,
        da.manager,
        da.regional_office,
        COUNT(DISTINCT f.opportunity_id)                        AS total_deals,
        COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))   AS won,
        COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))   AS lost,
        ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)           AS total_revenue,
        ROUND(AVG(IF(f.is_won, f.days_to_close, NULL)), 1)      AS avg_days_to_close
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_agent           da ON f.agent_key = da.agent_key
    GROUP BY da.sales_agent, da.manager, da.regional_office
),
rep_ranked AS (
    SELECT *,
        ROUND(won / NULLIF(won + lost, 0) * 100, 1)           AS win_rate_pct,
        NTILE(10) OVER (ORDER BY won / NULLIF(won + lost, 0) DESC) AS decile
    FROM rep_base
)
SELECT sales_agent, manager, regional_office, win_rate_pct, total_revenue, avg_days_to_close
FROM rep_ranked WHERE decile = 1
ORDER BY win_rate_pct DESC;


-- ─────────────────────────────────────────────────────────────
-- 3. BOTTOM 10% PERFORMERS
-- ─────────────────────────────────────────────────────────────
WITH rep_base AS (
    SELECT
        da.sales_agent,
        da.manager,
        da.regional_office,
        COUNT(DISTINCT f.opportunity_id)                        AS total_deals,
        COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))   AS won,
        COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))   AS lost,
        ROUND(SUM(IF(f.is_won, f.close_value, 0)), 0)           AS total_revenue,
        ROUND(AVG(IF(f.is_won, f.days_to_close, NULL)), 1)      AS avg_days_to_close
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_agent           da ON f.agent_key = da.agent_key
    GROUP BY da.sales_agent, da.manager, da.regional_office
),
rep_ranked AS (
    SELECT *,
        ROUND(won / NULLIF(won + lost, 0) * 100, 1)           AS win_rate_pct,
        NTILE(10) OVER (ORDER BY won / NULLIF(won + lost, 0) DESC) AS decile
    FROM rep_base
)
SELECT sales_agent, manager, regional_office, win_rate_pct, total_revenue, avg_days_to_close
FROM rep_ranked WHERE decile = 10
ORDER BY win_rate_pct ASC;


-- ─────────────────────────────────────────────────────────────
-- 4. REVENUE RECOVERY IF BOTTOM DECILE REACHED AVERAGE WIN RATE
--    Quantifies the $ value of rep coaching uplift
-- ─────────────────────────────────────────────────────────────
WITH rep_base AS (
    SELECT
        da.sales_agent,
        COUNT(DISTINCT f.opportunity_id)                        AS total_deals,
        COUNT(DISTINCT IF(f.is_won,  f.opportunity_id, NULL))   AS won,
        COUNT(DISTINCT IF(f.is_lost, f.opportunity_id, NULL))   AS lost,
        ROUND(AVG(IF(f.is_won, f.close_value, NULL)), 0)        AS avg_acv
    FROM analytics.fact_opportunities f
    JOIN analytics.dim_agent           da ON f.agent_key = da.agent_key
    GROUP BY da.sales_agent
),
with_rates AS (
    SELECT *,
        won / NULLIF(won + lost, 0)                           AS win_rate,
        NTILE(10) OVER (ORDER BY won / NULLIF(won + lost, 0) DESC) AS decile
    FROM rep_base
),
overall_avg AS (
    SELECT AVG(won / NULLIF(won + lost, 0)) AS avg_win_rate FROM rep_base
)

SELECT
    r.sales_agent,
    ROUND(r.win_rate * 100, 1)                                AS current_win_rate_pct,
    ROUND(o.avg_win_rate * 100, 1)                            AS avg_win_rate_pct,
    r.total_deals,
    r.avg_acv,
    -- Additional won deals if they reached average win rate
    ROUND((o.avg_win_rate - r.win_rate) * (r.won + r.lost))   AS additional_won_deals,
    -- Revenue lift
    ROUND((o.avg_win_rate - r.win_rate) * (r.won + r.lost) * r.avg_acv, 0)
                                                              AS est_revenue_lift
FROM with_rates r
CROSS JOIN overall_avg o
WHERE r.decile = 10
ORDER BY est_revenue_lift DESC;
