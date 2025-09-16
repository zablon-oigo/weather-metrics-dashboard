import os
import json
import requests
import pymysql
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime, timedelta

KAFKA_TOPIC="forecast"

API_KEY = Variable.get("API_KEY")
CITY = "Nairobi"
BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "user": os.getenv("MYSQL_USER", "test"),
    "password": os.getenv("MYSQL_PASSWORD", "pass"),
    "database": os.getenv("MYSQL_DB", "testDB"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
}

@dag(
    dag_id="weather_ingest_mysql",
    description="Fetch weather forecast",
    schedule="0 */3 * * *", 
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["weather", "kafka", "mysql"],
    default_args={
        "owner": "test",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)

def weather_ingest_mysql_dag():

    @task()
    def fetch_weather():
        response = requests.get(BASE_URL)
        response.raise_for_status()
        data = response.json()

        forecasts = []
        for entry in data.get("list", []):
            forecasts.append({
                "datetime": entry["dt_txt"],
                "temp": entry["main"]["temp"],
                "temp_min": entry["main"]["temp_min"],
                "temp_max": entry["main"]["temp_max"],
                "feels_like": entry["main"]["feels_like"],
                "pressure": entry["main"]["pressure"],
                "humidity": entry["main"]["humidity"],
                "weather_main": entry["weather"][0]["main"],
                "weather_description": entry["weather"][0]["description"],
                "weather_icon": entry["weather"][0]["icon"],
                "clouds": entry["clouds"]["all"],
                "rain": entry.get("rain", {}).get("3h", 0),
            })

        return {
            "city": data["city"]["name"],
            "country": data["city"]["country"],
            "forecast": forecasts,
        }


    produce_task = ProduceToTopicOperator(
    task_id="produce_weather",
    kafka_config_id="kafka_default", 
    topic=KAFKA_TOPIC,
    value="{{ ti.xcom_pull(task_ids='fetch_weather') | tojson }}",
    )

def save_to_mysql(message, **kwargs):
        payload = json.loads(message.value().decode("utf-8"))

        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_forecast (
                id INT AUTO_INCREMENT PRIMARY KEY,
                datetime DATETIME UNIQUE,
                temp FLOAT,
                temp_min FLOAT,
                temp_max FLOAT,
                feels_like FLOAT,
                pressure FLOAT,
                humidity FLOAT,
                weather_main VARCHAR(50),
                weather_description VARCHAR(100),
                weather_icon VARCHAR(10),
                clouds FLOAT,
                rain FLOAT,
                city VARCHAR(50),
                country VARCHAR(10),
                INDEX idx_datetime (datetime)
            )
        """)


