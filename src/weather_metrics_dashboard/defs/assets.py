import pandas as pd

import dagster as dg

sample_data_file = "src/weather_metrics_dashboard/defs/data/sample_data.csv"
processed_data_file = "src/weather_metrics_dashboard//defs/data/processed_data.csv"


@dg.asset
def processed_data():

    df = pd.read_csv(sample_data_file)

    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    df.to_csv(processed_data_file, index=False)
    return "Data loaded successfully"