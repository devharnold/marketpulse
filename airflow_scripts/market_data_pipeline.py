from datetime import datetime, timedelta
from pathlib import Path
import time

import pendulum
import yaml

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.ingestion.provider_ingestion import fetch_daily_data


localtz = pendulum.timezone("Africa/Nairobi")


@dag(
    dag_id="marketpulse_stock_ingestion",
    schedule="0 6 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz=localtz),
    catchup=False,
    default_args={
        "owner": "marketpulse",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["marketpulse", "finance", "data"],
)
def start_pipeline():

    @task
    def get_symbols():
        config_path = Path("/opt/airflow/config/stocks.yml")

        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        symbols = config["stocks"]
        print(f"Stocks configured: {symbols}")

        return symbols

    @task
    def fetch_and_load_stocks(symbols):

        hook = PostgresHook(
            postgres_conn_id="marketpulse_postgres"
        )

        sql = """
            INSERT INTO raw.stock_prices (
                symbol,
                date,
                open,
                high,
                low,
                close,
                volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """

        for index, symbol in enumerate(symbols):
            print(f"Fetching {symbol}...")

            try:
                data = fetch_daily_data(symbol)
                print(
                    f"Fetched {len(data)} records for {symbol}"
                )

                for record in data:
                    hook.run(
                        sql,
                        parameters=(
                            record["symbol"],
                            record["date"],
                            record["open"],
                            record["high"],
                            record["low"],
                            record["close"],
                            record["volume"],
                        ),
                    )

                print(
                    f"Successfully loaded {symbol} into PostgreSQL"
                )

            except Exception as e:

                print(
                    f"Failed to process {symbol}: {str(e)}"
                )

                raise

            # Wait before requesting the next symbol.
            # This prevents us from hammering Alpha Vantage.
            if index < len(symbols) - 1:
                print("Waiting 15 seconds before next API request...")
                time.sleep(15)

    symbols = get_symbols()

    fetch_and_load_stocks(symbols)


start_pipeline()