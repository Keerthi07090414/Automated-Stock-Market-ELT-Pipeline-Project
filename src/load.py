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
        # dim_stock: upload to a staging table, then insert-if-not-exists
        stocks.to_sql("dim_stock_staging", conn, if_exists="replace", index=False,
                       method="multi", chunksize=500)
        conn.execute(text("""
            INSERT INTO dim_stock (symbol, company_name)
            SELECT symbol, company_name FROM dim_stock_staging
            ON CONFLICT (symbol) DO NOTHING
        """))
        conn.execute(text("DROP TABLE dim_stock_staging"))

        # dim_date: same pattern
        dates.to_sql("dim_date_staging", conn, if_exists="replace", index=False,
                      method="multi", chunksize=500)
        conn.execute(text("""
            INSERT INTO dim_date (date_key, year, month, day, weekday)
            SELECT date_key, year, month, day, weekday FROM dim_date_staging
            ON CONFLICT (date_key) DO NOTHING
        """))
        conn.execute(text("DROP TABLE dim_date_staging"))

        # fact_prices: same pattern (already worked before)
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

    print(f"Load complete. Processed {len(fact)} fact rows (duplicates safely skipped).")


if __name__ == "__main__":
    load()