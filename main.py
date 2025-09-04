import requests
import os

API_KEY = os.getenv("API_KEY")
CITY = "Nairobi"
CNT = 16  
BASE_URL = f"http://pro.openweathermap.org/data/2.5/forecast/daily?q={CITY}&cnt={CNT}&appid={API_KEY}&units=metric"

def weather_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}

forecast = weather_data()
print(forecast)
