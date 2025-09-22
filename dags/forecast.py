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
    weather = fetch_weather()

    produce_task = ProduceToTopicOperator(
            task_id="produce_weather",
            kafka_config_id="kafka_default",
            topic=KAFKA_TOPIC,
            value="{{ ti.xcom_pull(task_ids='fetch_weather') | tojson }}",
        )
    weather = fetch_weather()
    weather >> produce_task 
dag = weather_ingest_mysql_dag()