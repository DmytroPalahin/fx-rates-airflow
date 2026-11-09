-- Core table for ECB reference rates. Safe to run many times.

-- Several DAG runs (e.g. a backfill) may start at once: serialize DDL with a
-- transaction-level advisory lock so concurrent CREATE ... IF NOT EXISTS do not race.
SELECT pg_advisory_xact_lock(hashtext('fx_ddl'));

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.ecb_rates (
    rate_date  date          NOT NULL,
    currency   char(3)       NOT NULL,
    rate       numeric(20,8) NOT NULL,
    loaded_at  timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, currency)
);

COMMENT ON TABLE core.ecb_rates IS
    'ECB reference rates: units of currency per 1 EUR. One row per TARGET business day and currency.';
