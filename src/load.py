import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

PROCESSED_PATH = os.path.join("data", "processed", "prices_transformed.parquet")


def run_ddl(engine):
    with open(os.path.join("sql", "create_tables.sql")) as f:
        ddl = f.read()
    with engine.begin() as conn:
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))


def load():
    engine = create_engine(DB_URL)
    run_ddl(engine)

    df = pd.read_parquet(PROCESSED_PATH)

    # dim_stock
    stocks = df[["symbol"]].drop_duplicates()
    stocks["company_name"] = stocks["symbol"].str.replace(".NS", "", regex=False)

    # dim_date
    dates = df[["date"]].drop_duplicates().rename(columns={"date": "date_key"})
    dates["date_key"] = pd.to_datetime(dates["date_key"])
    dates["year"] = dates["date_key"].dt.year
    dates["month"] = dates["date_key"].dt.month
    dates["day"] = dates["date_key"].dt.day
    dates["weekday"] = dates["date_key"].dt.day_name()

    # fact_prices
    fact = df.rename(columns={"date": "date_key"})[
        [
            "symbol", "date_key", "open", "high", "low", "close", "volume",
            "daily_return_pct", "moving_avg_7d", "moving_avg_30d",
            "volatility_7d", "is_price_jump_flag",
        ]
    ]

    with engine.begin() as conn:
        stocks.to_sql("dim_stock", conn, if_exists="append", index=False,
                       method="multi", chunksize=500)
        dates.to_sql("dim_date", conn, if_exists="append", index=False,
                      method="multi", chunksize=500)

    # Avoid duplicate-key errors on dim tables from repeated loads
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM dim_stock a USING dim_stock b
            WHERE a.ctid < b.ctid AND a.symbol = b.symbol
        """))
        conn.execute(text("""
            DELETE FROM dim_date a USING dim_date b
            WHERE a.ctid < b.ctid AND a.date_key = b.date_key
        """))

    with engine.begin() as conn:
        fact.to_sql("fact_prices_staging", conn, if_exists="replace", index=False,
                     method="multi", chunksize=500)
        conn.execute(text("""
            INSERT INTO fact_prices (symbol, date_key, open, high, low, close,
                volume, daily_return_pct, moving_avg_7d, moving_avg_30d,
                volatility_7d, is_price_jump_flag)
            SELECT symbol, date_key, open, high, low, close, volume,
                daily_return_pct, moving_avg_7d, moving_avg_30d,
                volatility_7d, is_price_jump_flag
            FROM fact_prices_staging
            ON CONFLICT (symbol, date_key) DO NOTHING
        """))
        conn.execute(text("DROP TABLE fact_prices_staging"))

    print(f"Loaded {len(fact)} rows into fact_prices")


if __name__ == "__main__":
    load()