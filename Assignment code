import requests
import snowflake.connector
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime, timedelta

@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def hw4_stock_dag():

    @task
    def fetch_stock_data(symbol):
        api_key = Variable.get("alpha_vantage_api_key")
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response = requests.get(url)
        return response.json()

    @task
    def transform_data(stock_data):
        records = []
        for d, stock_info in stock_data["Time Series (Daily)"].items():
            records.append([
                float(stock_info["1. open"]),
                float(stock_info["2. high"]),
                float(stock_info["3. low"]),
                float(stock_info["4. close"]),
                int(stock_info["5. volume"]),
                d
            ])
        return records

    @task
    def load_to_snowflake(records, symbol):
        conn = snowflake.connector.connect(
            user=Variable.get("snowflake_username"),
            password=Variable.get("snowflake_password"),
            account=Variable.get("snowflake_account"),
            warehouse="compute_wh",
            database="dev"
        )
        cursor = conn.cursor()
        target_table = "dev.raw.stocks"

        try:
            cursor.execute("BEGIN;")
            cursor.execute(f"DELETE FROM {target_table} WHERE symbol = '{symbol}';")
            for r in records:
                sql = f"INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (symbol, r[5], r[0], r[1], r[2], r[3], r[4]))
            cursor.execute("COMMIT;")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            raise e
        finally:
            cursor.close()
            conn.close()

    symbol = "AMZN"
    stock_data = fetch_stock_data(symbol)
    transformed_data = transform_data(stock_data)
    load_to_snowflake(transformed_data, symbol)

dag_instance = hw4_stock_dag()
