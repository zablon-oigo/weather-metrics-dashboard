from dagster_duckdb import DuckDBResource
import dagster as dg


@dlt.source
def open_weather_source(start_date: str, end_date: str, api_key: str):
    @dlt.resource
    def fetch_weather_data():
        url = ""
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": api_key,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

    return fetch_weather_data
