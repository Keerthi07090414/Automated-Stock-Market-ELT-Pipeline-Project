import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)


def run_checks():
    engine = create_engine(DB_URL)
    issues = []

    with engine.connect() as conn:
        # Check 1: nulls in critical columns
        null_check = conn.execute(text("""
            SELECT COUNT(*) FROM fact_prices
            WHERE close IS NULL OR symbol IS NULL OR date_key IS NULL
        """)).scalar()
        if null_check > 0:
            issues.append(f"{null_check} rows have NULL close/symbol/date_key")

        # Check 2: duplicate (symbol, date_key) pairs
        dup_check = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT symbol, date_key, COUNT(*) c
                FROM fact_prices
                GROUP BY symbol, date_key
                HAVING COUNT(*) > 1
            ) t
        """)).scalar()
        if dup_check > 0:
            issues.append(f"{dup_check} duplicate (symbol, date_key) pairs found")

        # Check 3: negative or zero prices
        bad_price_check = conn.execute(text("""
            SELECT COUNT(*) FROM fact_prices WHERE close <= 0
        """)).scalar()
        if bad_price_check > 0:
            issues.append(f"{bad_price_check} rows have close price <= 0")

        # Check 4: rows already flagged as abnormal price jumps
        jump_check = conn.execute(text("""
            SELECT COUNT(*) FROM fact_prices WHERE is_price_jump_flag = TRUE
        """)).scalar()
        if jump_check > 0:
            issues.append(f"{jump_check} rows flagged as abnormal price jumps (>15% daily move)")

        # Check 5: total row count sanity check
        total_rows = conn.execute(text("SELECT COUNT(*) FROM fact_prices")).scalar()

    print(f"Data quality check complete. Total rows in fact_prices: {total_rows}")
    if issues:
        print("Issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("No data quality issues found.")

    return issues


if __name__ == "__main__":
    run_checks()