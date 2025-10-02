from dagster_duckdb import DuckDBResource
import dagster as dg
import os
from dotenv import load_dotenv
import dlt

load_dotenv()

@dlt.source
def open_weather_source(start_date: str, end_date: str, api_key: str):
    @dlt.resource
    def fetch_weather_data():
        url = ""
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": os.getenv("API_KEY"),
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
pipeline=dlt.pipeline(
        pipeline_name="weather_pipeline",
        destination=dlt.destinations.duckdb(os.getenv("DUCKDB_DATABASE")),
        dataset_name="weather"
    )