-- Rebuild marts.fx_daily_changes for [start_date, end_date] (idempotent: delete + insert).
-- Parameters come from the resolve_date_range task through XCom.

DELETE FROM marts.fx_daily_changes
WHERE rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date;

INSERT INTO marts.fx_daily_changes
    (source, rate_date, currency, rate, prev_rate_date, prev_rate, pct_change)
SELECT
    source,
    rate_date,
    currency,
    rate,
    prev_rate_date,
    prev_rate,
    -- Only compare with a *real* previous publication. If history has a hole
    -- (e.g. only part of it was backfilled), the "previous" loaded day can be
    -- months away and the % change would be meaningless.
    CASE
        WHEN rate_date - prev_rate_date <= max_gap_days
        THEN ROUND((rate / prev_rate - 1) * 100, 4)
    END AS pct_change
FROM (
    -- LAG() must see the full history, so filter by date only *after* the window.
    -- ECB: up to 5 days between publications (Easter: Thu -> Tue, Christmas).
    SELECT 'ecb' AS source, 5 AS max_gap_days, rate_date, currency, rate,
           LAG(rate_date) OVER w AS prev_rate_date,
           LAG(rate)      OVER w AS prev_rate
    FROM core.ecb_rates
    WINDOW w AS (PARTITION BY currency ORDER BY rate_date)

    UNION ALL

    -- NBU: a rate for every calendar day.
    SELECT 'nbu' AS source, 1 AS max_gap_days, rate_date, currency, rate,
           LAG(rate_date) OVER w AS prev_rate_date,
           LAG(rate)      OVER w AS prev_rate
    FROM core.nbu_rates
    WINDOW w AS (PARTITION BY currency ORDER BY rate_date)
) AS with_prev
WHERE rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date;
