import os
import pandas as pd
from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity

data_folder = '/opt/dagster/app/data'

@asset
def silver_null_handling():
    csv_path = [f for f in os.listdir(data_folder) if f.startswith('Bronze_Data_') and f.endswith('.csv')]
    latest_bronze_file = max(csv_path, key=lambda x: os.path.getctime(os.path.join(data_folder, x)))
    latest_bronze_file_path = os.path.join(data_folder, latest_bronze_file)

    bronze_df = pd.read_csv(latest_bronze_file_path)
    silver_data_drop_null_df = bronze_df.dropna(axis=0, subset=['Price_Per_Unit', 'Total_Price'], thresh=1)
    silver_data_drop_null_df.to_csv("/opt/dagster/app/data/silver_data_null_handling.csv", index=False)

    return silver_data_drop_null_df

@asset_check(asset=silver_null_handling)
def silver_data_has_no_nulls(silver_null_handling):
    has_nulls = silver_null_handling[['Price_Per_Unit', 'Total_Price']].isnull().any().any()

    if has_nulls:
        yield AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Warning! NULL values detected in DataFrame!"
        )
    else:
        yield AssetCheckResult(
            passed=True,
            description="No NULL values in DataFrame."
        )
