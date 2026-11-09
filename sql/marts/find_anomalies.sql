-- Rows that deserve an alert inside [start_date, end_date], for watchlist currencies only.

SELECT 'daily_change'           AS kind,
       source,
       rate_date,
       currency,
       pct_change               AS value,
       %(change_threshold)s     AS threshold
FROM marts.fx_daily_changes
WHERE rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date
  AND currency = ANY(%(currencies)s)
  AND ABS(pct_change) >= %(change_threshold)s

UNION ALL

SELECT 'cross_rate_deviation'   AS kind,
       'ecb_vs_nbu'             AS source,
       rate_date,
       currency,
       deviation_pct            AS value,
       %(deviation_threshold)s  AS threshold
FROM marts.fx_cross_rates
WHERE rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date
  AND currency = ANY(%(currencies)s)
  AND ABS(deviation_pct) >= %(deviation_threshold)s

ORDER BY rate_date, kind, currency;
