import os
import json
import requests
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.provider.apache.kafka.operators.consume import ConsumeFromTopicOperator

KAFKA_TOPIC="forecast"

API_KEY = Variable.get("API_KEY")
CITY = "Nairobi"
BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/usr/local/airflow/data/weather.duckdb")

def weather_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        data = response.json()
        forecasts = []
        for entry in data.get("list", []):
            forecast = {
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
            }
            forecasts.append(forecast)

        result = {
            "city": data["city"]["name"],
            "country": data["city"]["country"],
            "forecast": forecasts,
        }

        print(json.dumps(result, indent=4))
        return result
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

