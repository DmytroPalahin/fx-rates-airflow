-- Rebuild marts.fx_cross_rates for [start_date, end_date] (idempotent: delete + insert).
-- Only days where both sources have data (= ECB business days) produce rows.

DELETE FROM marts.fx_cross_rates
WHERE rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date;

INSERT INTO marts.fx_cross_rates
    (rate_date, currency, ecb_per_eur, nbu_uah_per_eur, nbu_uah_per_unit,
     implied_uah_per_unit, deviation_pct)
SELECT
    ecb.rate_date,
    ecb.currency,
    ecb.rate                                                    AS ecb_per_eur,
    nbu_eur.rate                                                AS nbu_uah_per_eur,
    nbu.rate                                                    AS nbu_uah_per_unit,
    ROUND(nbu_eur.rate / ecb.rate, 8)                           AS implied_uah_per_unit,
    ROUND((nbu.rate / (nbu_eur.rate / ecb.rate) - 1) * 100, 4)  AS deviation_pct
FROM core.ecb_rates AS ecb
JOIN core.nbu_rates AS nbu
    ON nbu.rate_date = ecb.rate_date AND nbu.currency = ecb.currency
JOIN core.nbu_rates AS nbu_eur
    ON nbu_eur.rate_date = ecb.rate_date AND nbu_eur.currency = 'EUR'
WHERE ecb.rate_date BETWEEN %(start_date)s::date AND %(end_date)s::date;
