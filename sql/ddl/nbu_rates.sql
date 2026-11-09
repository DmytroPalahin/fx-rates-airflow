-- Core table for NBU official rates. Safe to run many times.

-- See ecb_rates.sql: serialize concurrent DDL.
SELECT pg_advisory_xact_lock(hashtext('fx_ddl'));

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.nbu_rates (
    rate_date  date          NOT NULL,
    currency   char(3)       NOT NULL,
    rate       numeric(20,8) NOT NULL,
    loaded_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency)
);

COMMENT ON TABLE core.nbu_rates IS
    'NBU official rates: UAH per 1 unit of currency. One row per calendar day and currency.';
