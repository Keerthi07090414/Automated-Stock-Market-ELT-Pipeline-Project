CREATE TABLE IF NOT EXISTS dim_stock (
    symbol      VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    DATE PRIMARY KEY,
    year        INT,
    month       INT,
    day         INT,
    weekday     VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS fact_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20) REFERENCES dim_stock(symbol),
    date_key        DATE REFERENCES dim_date(date_key),
    open            NUMERIC(12,2),
    high            NUMERIC(12,2),
    low             NUMERIC(12,2),
    close           NUMERIC(12,2),
    volume          BIGINT,
    daily_return_pct NUMERIC(6,2),
    moving_avg_7d   NUMERIC(12,2),
    moving_avg_30d  NUMERIC(12,2),
    volatility_7d   NUMERIC(6,2),
    is_price_jump_flag BOOLEAN,
    UNIQUE (symbol, date_key)
);