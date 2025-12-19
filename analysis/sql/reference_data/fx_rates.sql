-- Create FX Rates table (SAR as base currency)
CREATE TABLE IF NOT EXISTS app_core.fx_rates (
    currency TEXT NOT NULL,
    rate_to_sar NUMERIC NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    PRIMARY KEY (currency, valid_from)
);

-- Optional: clear old rates (use carefully in prod)
-- TRUNCATE TABLE app_core.fx_rates;

-- Insert base SAR rate
INSERT INTO app_core.fx_rates (currency, rate_to_sar, valid_from, valid_to)
VALUES ('SAR', 1.0, '2024-01-01', '2024-12-31')
ON CONFLICT DO NOTHING;
