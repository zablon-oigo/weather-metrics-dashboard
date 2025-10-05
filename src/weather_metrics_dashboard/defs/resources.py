import dagster as dg
from  dagster_duckdb import DuckDBResource

database_resource=DuckDBResource(database="/tmp/weather.duckdb")


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "duckdb": database_resource,
    })

