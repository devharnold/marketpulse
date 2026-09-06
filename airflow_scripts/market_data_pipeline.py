from datetime import datetime, timedelta
from pathlib import Path
import pendulum

import yaml

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.ingestion.provider_ingestion import fetch_daily_data

localtz = pendulum.timezone("Africa/Nairobi")


@dag(
    dag_id="marketpulse_stock_ingestion",
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 1, 1, tz=localtz),
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

        return config["stocks"]

    @task
    def fetch_stock_data(symbol: str):
        data = fetch_daily_data(symbol)

        return data

    @task
    def load_to_postgres(data):
        hook = PostgresHook(
            postgres_conn_id="marketpulse_postgres"
        )

        sql = """
            INSERT INTO stock_prices (
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

        for records in data:
            for record in records:
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

    # Build the task dependency graph
    symbols = get_symbols()
    data = fetch_stock_data.expand(symbol=symbols)
    load_to_postgres(data)


start_pipeline()
