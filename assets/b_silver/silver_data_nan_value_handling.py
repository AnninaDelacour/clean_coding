import os
import pandas as pd
from datetime import datetime
from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
from .create_table import create_table_and_validate

data_folder = '/opt/dagster/app/data'

@asset
def silver_nan_and_validated(silver_sorted):
    silver_data_df = silver_sorted.replace('N/A', pd.NA)

    silver_data_df['Date_Time'] = pd.to_datetime(silver_data_df['Date_Time'], errors='coerce')
    silver_data_df['Ingested_Timestamp'] = pd.to_datetime(silver_data_df['Ingested_Timestamp'], errors='coerce')

    silver_data_df_validated = create_table_and_validate(silver_data_df)

    timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
    silver_data_final_csv_path = os.path.join(data_folder, f'silver_data_final_{timestamp}.csv')
    silver_data_df_validated.to_csv(silver_data_final_csv_path, index=False)

    return silver_data_df_validated

@asset_check(asset=silver_nan_and_validated)
def silver_data_validated(silver_nan_and_validated):
    if silver_nan_and_validated.isnull().values.any():
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
