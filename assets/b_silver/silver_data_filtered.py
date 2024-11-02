from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
import pandas as pd
import os

data_folder = '/opt/dagster/app/data'

@asset
def silver_filtered(silver_null_handling):
    # Verwende die Ausgabe von silver_null_handling direkt als DataFrame
    silver_data_df = silver_null_handling
    silver_filtered_df = silver_data_df[silver_data_df['Product'] == 'Eingelegte Gurken']
    silver_filtered_df.to_csv("/opt/dagster/app/data/silver_data_filtered.csv", index=False)

    return silver_filtered_df

@asset_check(asset=silver_filtered)
def silver_data_check_product_value(silver_filtered):
    if (silver_filtered['Product'] != 'Eingelegte Gurken').any():
        yield AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Warning! Es gibt Produkte, die nicht 'Eingelegte Gurken' sind."
        )
    else:
        yield AssetCheckResult(
            passed=True,
            description="Alle Produkte sind 'Eingelegte Gurken'."
        )
