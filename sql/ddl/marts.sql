-- Mart tables. Safe to run many times.

SELECT pg_advisory_xact_lock(hashtext('fx_ddl'));

CREATE SCHEMA IF NOT EXISTS marts;

-- Day-over-day change of every rate, for both sources.
-- "Previous" means the previous published day (ECB skips weekends and holidays).
CREATE TABLE IF NOT EXISTS marts.fx_daily_changes (
    source          text          NOT NULL,  -- 'ecb' | 'nbu'
    rate_date       date          NOT NULL,
    currency        char(3)       NOT NULL,
    rate            numeric(20,8) NOT NULL,
    prev_rate_date  date,
    prev_rate       numeric(20,8),
    pct_change      numeric(12,4),           -- NULL: first known day or a gap in loaded history
    built_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (source, rate_date, currency)
);

-- Cross check of the two sources: the NBU rate of a currency vs. the rate implied
-- by the ECB (currency per EUR) and the NBU EUR rate (UAH per EUR).
--   implied UAH per unit = NBU UAH/EUR / ECB currency/EUR
-- Note: the NBU sets the rate for day D on D-1, the ECB fixes it on D, so a
-- deviation of a few tenths of a percent is normal.
CREATE TABLE IF NOT EXISTS marts.fx_cross_rates (
    rate_date             date          NOT NULL,
    currency              char(3)       NOT NULL,
    ecb_per_eur           numeric(20,8) NOT NULL,
    nbu_uah_per_eur       numeric(20,8) NOT NULL,
    nbu_uah_per_unit      numeric(20,8) NOT NULL,
    implied_uah_per_unit  numeric(20,8) NOT NULL,
    deviation_pct         numeric(12,4) NOT NULL,
    built_at              timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency)
);
