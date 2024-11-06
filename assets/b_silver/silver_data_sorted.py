from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
from .create_table import create_table_and_validate
from datetime import datetime
import pandas as pd
import os

data_folder = '/opt/dagster/app/data'

@asset
def silver_sorted(silver_filtered):
    silver_data_df = silver_filtered.sort_values('Transaction_ID', ascending=False)

    silver_data_df['Date_Time'] = pd.to_datetime(silver_data_df['Date_Time'], errors='coerce')
    silver_data_df['Ingested_Timestamp'] = pd.to_datetime(silver_data_df['Ingested_Timestamp'], errors='coerce')

    silver_data_final = create_table_and_validate(silver_data_df)

    timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
    silver_data_final_csv_path = os.path.join(data_folder, f'silver_data_final_{timestamp}.csv')
    silver_data_final.to_csv(silver_data_final_csv_path, index=False)

    return silver_data_final



@asset_check(asset=silver_sorted)
def silver_data_is_sorted(silver_sorted):
    if silver_sorted.isnull().values.any():
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
        