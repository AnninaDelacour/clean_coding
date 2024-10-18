import os
import pandas as pd
from datetime import datetime
from dagster import asset
from .create_table import create_table_and_validate


data_folder = '/opt/dagster/app/data'


@asset
def bronze_data():
    '''
    In bronze_data werden die Rohdaten eingelesen (hier als CSV).
    Die Daten werden mittels Pandera validiert und als vorgegebene Struktur abgespeichert.
    Der Zeitpunkt, ab dem die Daten im System eingelesen werden, wird mit einer neuen Spalte "Ingested_Timestamp" festgehalten.
    '''
    
    csv_path = os.path.join(data_folder, 'Raw_Synthetic_Transaction_Data_Three_Months.csv')
    df = pd.read_csv(csv_path)

    df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')

    df['Ingested_Timestamp'] = datetime.now()

    validated_df = create_table_and_validate(df)

    timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
    bronze_csv_path = os.path.join(data_folder, f'Bronze_Data_{timestamp}.csv')
    validated_df.to_csv(bronze_csv_path, index=False)

    return validated_df
