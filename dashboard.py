import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

st.set_page_config(page_title="NSE Stock ELT Dashboard", layout="wide")


@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(DB_URL)
    query = """
        SELECT symbol, date_key, open, high, low, close, volume,
               daily_return_pct, moving_avg_7d, moving_avg_30d,
               volatility_7d, is_price_jump_flag
        FROM fact_prices
        ORDER BY date_key
    """
    return pd.read_sql(query, engine)


df = load_data()

st.title("📈 NSE Stock Market ELT Dashboard")
st.caption("Automated pipeline: yfinance → PySpark → PostgreSQL → Airflow")

# --- Sidebar filters ---
st.sidebar.header("Filters")
symbols = sorted(df["symbol"].unique())
selected_symbols = st.sidebar.multiselect("Select stocks", symbols, default=symbols[:5])

filtered = df[df["symbol"].isin(selected_symbols)]

# --- Top metrics ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total rows", f"{len(df):,}")
col2.metric("Stocks tracked", df["symbol"].nunique())
col3.metric("Date range", f"{df['date_key'].min()} → {df['date_key'].max()}")
col4.metric("Price jump flags", int(df["is_price_jump_flag"].sum()))

st.divider()

# --- Price trend chart ---
st.subheader("Closing Price Trend")
fig_price = px.line(
    filtered, x="date_key", y="close", color="symbol",
    labels={"date_key": "Date", "close": "Close Price (₹)"},
)
st.plotly_chart(fig_price, use_container_width=True)

# --- Moving averages ---
st.subheader("7-Day vs 30-Day Moving Average")
selected_single = st.selectbox("Pick one stock to inspect", selected_symbols) if selected_symbols else None
if selected_single:
    single_df = filtered[filtered["symbol"] == selected_single]
    fig_ma = px.line(
        single_df, x="date_key", y=["close", "moving_avg_7d", "moving_avg_30d"],
        labels={"date_key": "Date", "value": "Price (₹)"},
    )
    st.plotly_chart(fig_ma, use_container_width=True)

# --- Daily returns ---
st.subheader("Daily Return % by Stock")
fig_returns = px.box(filtered, x="symbol", y="daily_return_pct")
st.plotly_chart(fig_returns, use_container_width=True)

# --- Raw data table ---
with st.expander("View raw data"):
    st.dataframe(filtered.sort_values("date_key", ascending=False))