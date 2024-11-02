from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
import pandas as pd
import os

data_folder = '/opt/dagster/app/data'

@asset
def silver_sorted(silver_filtered):
    silver_data_df = silver_filtered.sort_values('Transaction_ID', ascending=False)
    silver_data_df.to_csv("/opt/dagster/app/data/silver_sorted_df.csv", index=False)

    return silver_data_df

@asset_check(asset=silver_sorted)
def silver_data_is_sorted(silver_sorted):
    if silver_sorted['Transaction_ID'].is_monotonic_decreasing:
        yield AssetCheckResult(
            passed=True,
            description="Die Daten sind in absteigender Reihenfolge sortiert."
        )
    else:
        yield AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Warning! Die Daten sind nicht in absteigender Reihenfolge sortiert."
        )
