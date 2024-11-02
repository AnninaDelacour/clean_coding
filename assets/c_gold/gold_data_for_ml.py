import os
import pandas as pd
from assets.c_gold.create_ml_table import create_table_and_validate
from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
from datetime import datetime

data_folder = '/opt/dagster/app/data'

@asset
def gold_data_ml(silver_nan_and_validated):
    gold_data_df = silver_nan_and_validated.dropna()

    # Behalte den DataFrame in `gold_data_df_final`
    gold_data_df_final = create_table_and_validate(gold_data_df)

    # Speichere den Dateipfad separat
    timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
    gold_data_df_final_path = os.path.join(data_folder, f'gold_data_ml_{timestamp}.csv')
    
    # Speichere den DataFrame in eine CSV-Datei
    gold_data_df_final.to_csv(gold_data_df_final_path, index=False)

    return gold_data_df_final

@asset_check(asset=gold_data_ml)
def gold_data_ml_validated(gold_data_ml):
    if gold_data_ml.isnull().values.any():
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
