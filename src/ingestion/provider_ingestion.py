# Sample Data Ingestion script, will add tests later

import os
import requests
import json
from pathlib import Path
from dotenv import load_dotenv

#load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

BASE_API_URL = "https://www.alphavantage.co/query"


def fetch_daily_data(symbol: str) -> list[dict]:
    response = requests.get(
        BASE_API_URL,
        params={
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",
            "apikey": API_KEY,
        },
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    if "Error Message" in payload:
        raise RuntimeError(
            f"Alpha Vantage error for {symbol}: "
            f"{payload['Error Message']}"
        )

    if "Note" in payload:
        raise RuntimeError(
            f"Alpha Vantage API limit reached for {symbol}"
        )

    time_series = payload.get("Time Series (Daily)")

    if not time_series:
        raise RuntimeError(
            f"No data returned for {symbol}"
        )

    results = []

    for date, values in time_series.items():
        results.append(
            {
                "symbol": symbol,
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            }
        )

    return results


def save_raw_data(data):
    """Save the raw JSON data to a local JSON file."""

    #path = Path("../../data/raw/finapi.json")
    path = PROJECT_ROOT / "data" / "raw" / "finapi.json"

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    symbol = "IBM"
    data = fetch_daily_data(symbol)

    save_raw_data(data)

    print(f"Successfully ingested {len(data)} records for {symbol}")


# TODO:
# Load config/stocks.yaml in the DAG with yaml.safe_load()