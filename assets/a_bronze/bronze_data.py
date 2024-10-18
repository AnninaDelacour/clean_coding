import pandas as pd
from datetime import datetime
from dagster import asset
from .create_table import create_table_and_validate

@asset
def bronze_data():
    '''
    In bronze_data werden die Rohdaten eingelesen (hier als CSV).
    Die Daten werden mittels Pandera validiert und als vorgegebene Struktur abgespeichert.
    Der Zeitpunkt, ab dem die Daten im System eingelesen werden, wird mit einer neuen Spalte "Ingested_Timestamp" festgehalten.
    '''
    
    df = pd.read_csv('/opt/dagster/app/Synthetic_Transaction_Data.csv')

    df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')

    df['Ingested_Timestamp'] = datetime.now()

    validated_df = create_table_and_validate(df)

    bronze_csv_path = '/opt/dagster/app/Bronze_Data.csv'
    validated_df.to_csv(bronze_csv_path, index=False)

    return validated_df
