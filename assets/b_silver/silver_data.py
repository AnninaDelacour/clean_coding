import pandas as pd
from dagster import asset

@asset
def silver_data():
    # Lese die vom Bronze-Level generierte Datei
    df = pd.read_csv('/opt/dagster/app/data/bronze_output.csv')
    # Beispiel für eine Verfeinerung
    df = df[df['processed_column'] > 0]  # Filtere z.B. negative Werte
    # Speichere das Ergebnis im Silver-Level
    df.to_csv('/opt/dagster/app/data/silver_output.csv', index=False)
    return df
