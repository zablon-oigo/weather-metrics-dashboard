import requests
import os


API_KEY = os.getenv("API_KEY")
CITY = "Nairobi"
BASE_URL = f"http://api.openweathermap.org/data/2.5/forecast?q={CITY}&appid={API_KEY}&units=metric"

def weather_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}

forecast = weather_data()
print(forecast)

