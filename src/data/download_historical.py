"""
Download 3 years of hourly OHLCV data for major forex pairs using yfinance.
yfinance caps 1h interval at 730 days per request, so data is fetched in
two overlapping chunks and deduplicated before saving.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

INSTRUMENTS = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "AUD/USD": "AUDUSD=X",
    "USD/CAD": "USDCAD=X",
    "NZD/USD": "NZDUSD=X",
}

OUTPUT_DIR = Path("data/historical")
YEARS = 3
INTERVAL = "1h"
# yfinance hard cap for hourly data is 730 days per request
CHUNK_DAYS = 729


def fetch_instrument(ticker: str) -> pd.DataFrame:
    """Fetch up to YEARS years of hourly data by downloading in chunks."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=YEARS * 365)

    chunks = []
    chunk_end = end

    while chunk_end > start:
        chunk_start = max(chunk_end - timedelta(days=CHUNK_DAYS), start)
        df = yf.download(
            ticker,
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            interval=INTERVAL,
            progress=False,
            auto_adjust=True,
        )
        if not df.empty:
            # yfinance 1.x returns MultiIndex columns: (Price, Ticker) — flatten them
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            chunks.append(df)
        chunk_end = chunk_start

    if not chunks:
        return pd.DataFrame()

    combined = pd.concat(chunks)
    combined = combined[~combined.index.duplicated(keep="first")]
    combined.sort_index(inplace=True)
    return combined


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    succeeded = []
    failed = []

    for name, ticker in INSTRUMENTS.items():
        print(f"Downloading {name} ({ticker})...", end=" ", flush=True)
        try:
            df = fetch_instrument(ticker)

            if df.empty:
                raise ValueError("No data returned")

            out_path = OUTPUT_DIR / f"{name.replace('/', '_')}.csv"
            df.to_csv(out_path)

            print(f"{len(df):,} rows saved -> {out_path}")
            succeeded.append(name)

        except Exception as exc:
            print(f"FAILED ({exc})")
            failed.append(name)

    print("\n--- Summary ---")
    print(f"Succeeded : {len(succeeded)}/{len(INSTRUMENTS)}")
    if succeeded:
        print(f"  {', '.join(succeeded)}")
    if failed:
        print(f"Failed    : {len(failed)}/{len(INSTRUMENTS)}")
        print(f"  {', '.join(failed)}")


if __name__ == "__main__":
    main()
