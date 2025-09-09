from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import json
from datetime import datetime,timedelta
from dotenv import load_dotenv 

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITY = "Nairobi"
BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"

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
                "weather": {
                    "main": entry["weather"][0]["main"],
                    "description": entry["weather"][0]["description"],
                    "icon": entry["weather"][0]["icon"]
                },
                "clouds": entry["clouds"]["all"],
                "rain": entry.get("rain", {}).get("3h", 0)
            }
            forecasts.append(forecast)
        
        return {
            "city": data["city"],
            "forecast": forecasts
        }
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}

forecast = weather_data()

print(json.dumps(forecast, indent=4))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 7), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="weather_forecast_dag",
    default_args=default_args,
    schedule_interval="0 */3 * * *",  
    catchup=False,
    tags=["weather", "api"],
) as dag:
    

fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=weather_data
    )

fetch_weather