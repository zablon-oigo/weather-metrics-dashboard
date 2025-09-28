from dagster_duckdb import DuckDBResource
import dagster as dg





@dg.asset_check(asset="orders_aggregation")
def orders_aggregation_check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    table_name = "orders_aggregation"
    with duckdb.get_connection() as conn:
        row_count = conn.execute(f"select count(*) from {table_name}").fetchone()[0]

    if row_count == 0:
        return dg.AssetCheckResult(
            passed=False, metadata={"message": "Order aggregation check failed"}
        )

    return dg.AssetCheckResult(
        passed=True, metadata={"message": "Order aggregation check passed"}
    )

