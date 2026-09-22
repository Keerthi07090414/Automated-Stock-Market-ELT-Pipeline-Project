import os
from datetime import date

import pandas as pd
import yfinance as yf

# NSE tickers end with ".NS"
STOCKS = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "ITC.NS", "LT.NS", "WIPRO.NS", "HCLTECH.NS",
]

RAW_DIR = os.path.join("data", "raw")


def ingest(period: str = "1mo") -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    frames = []

    for symbol in STOCKS:
        df = yf.Ticker(symbol).history(period=period).reset_index()
        if df.empty:
            print(f"No data for {symbol}, skipping")
            continue
        df["symbol"] = symbol
        frames.append(df)

    data = pd.concat(frames, ignore_index=True)
    data.columns = [c.lower().replace(" ", "_") for c in data.columns]
    data["date"] = pd.to_datetime(data["date"]).dt.date

    out_path = os.path.join(RAW_DIR, f"prices_{date.today().isoformat()}.parquet")
    data.to_parquet(out_path, index=False)
    print(f"Saved {len(data)} rows to {out_path}")
    return out_path


if __name__ == "__main__":
    ingest()