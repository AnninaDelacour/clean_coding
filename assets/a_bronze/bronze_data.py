import os
import pandas as pd
from datetime import datetime
from dagster import asset, asset_check, AssetCheckResult, AssetCheckSeverity
from .create_table import create_table_and_validate


data_folder = '/opt/dagster/app/data'


@asset
def bronze_data():
    '''
    In bronze_data werden die Rohdaten aus der Central Processing Unit eingelesen, die von dort noch nicht validiert, 
    aber mit einem Zeitstempel (Ingested_Timestamp) versehen wurde.
    Die Daten werden mittels Pandera validiert und in der vorgegebene Struktur im DB bzw. Data Warehouse abgespeichert.
    '''
    
    csv_path = os.path.join(data_folder, 'Raw_Synthetic_Transaction_Data_Three_Months.csv')
    df = pd.read_csv(csv_path)

    df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')

    df['Ingested_Timestamp'] = pd.to_datetime(df['Ingested_Timestamp'], errors='coerce')

    validated_df = create_table_and_validate(df)

    timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
    bronze_csv_path = os.path.join(data_folder, f'Bronze_Data_{timestamp}.csv')
    validated_df.to_csv(bronze_csv_path, index=False)

    return validated_df


@asset_check(asset=bronze_data)
def check_transaction_id_not_null():
    '''
    Überprüft, ob die Transaction_ID Spalte in den Bronze-Daten keine NULL-Werte enthält.
    '''

    # Dynamisch den neuesten Bronze-CSV-Pfad finden
    bronze_files = [f for f in os.listdir(data_folder) if f.startswith('Bronze_Data_') and f.endswith('.csv')]
    latest_file = max(bronze_files, key=lambda x: os.path.getctime(os.path.join(data_folder, x)))
    latest_file_path = os.path.join(data_folder, latest_file)

    # CSV-Datei einlesen
    bronze_data_df = pd.read_csv(latest_file_path)

    # Überprüfen, ob Transaction_ID NULL-Werte enthält
    if bronze_data_df['Transaction_ID'].isnull().any():
        yield AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="NULL values detected: Missing values in column Transaction_ID!"
        )
    else:
        yield AssetCheckResult(
            passed=True,
            description="Transaction_ID column has no NULL values."
        )