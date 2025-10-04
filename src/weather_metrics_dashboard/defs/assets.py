import os
import requests
import dlt
from dotenv import load_dotenv

load_dotenv()

@dlt.source
def open_weather_source(city: str, api_key: str):
    @dlt.resource
    def fetch_weather_data():
        """
        Fetch current or forecast weather data from OpenWeather API
        and yield normalized JSON records.
        """
        url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            "q": city,
            "appid": api_key,
            "units": "metric"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        for entry in data["list"]:
            yield {
                "timestamp": entry["dt_txt"],
                "temperature": entry["main"]["temp"],
                "humidity": entry["main"]["humidity"],
                "pressure": entry["main"]["pressure"],
                "weather": entry["weather"][0]["description"],
                "wind_speed": entry["wind"]["speed"],
                "city": data["city"]["name"],
                "country": data["city"]["country"]
            }

    return fetch_weather_data

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="weather_pipeline",
        destination=dlt.destinations.duckdb(os.getenv("DUCKDB_DATABASE", "weather.duckdb")),
        dataset_name="weather",
    )
    source = open_weather_source(
        city=os.getenv("CITY", "Nairobi"),
        api_key=os.getenv("API_KEY")
    )

    load_info = pipeline.run(source)

    print(load_info)
