import requests
import os
from datetime import datetime

API_KEY = os.getenv("API_KEY")
CITY = "Nairobi"
CNT = 16  
BASE_URL = f"http://pro.openweathermap.org/data/2.5/forecast/daily?q={CITY}&cnt={CNT}&appid={API_KEY}&units=metric"

def weather_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        data = response.json()
        forecasts = []
        for day in data.get("list", []):
            forecast = {
                "date": datetime.fromtimestamp(day["dt"]).strftime("%Y-%m-%d"),
                "temp_day": day["temp"]["day"],
                "temp_min": day["temp"]["min"],
                "temp_max": day["temp"]["max"],
                "weather": day["weather"][0]["description"]
            }
            forecasts.append(forecast)
        return forecasts
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}

forecast = weather_data()
print(forecast)
