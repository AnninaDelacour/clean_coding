import pandas as pd
from dagster import asset

@asset
def gold_data():
    # Lese die vom Silver-Level generierte Datei
    df = pd.read_csv('/opt/dagster/app/data/silver_output.csv')
    # Beispiel für den letzten Verarbeitungsschritt
    df['final_result'] = df['processed_column'] * 100  # Ein weiteres Beispiel
    # Speichere das Endergebnis im Gold-Level
    df.to_csv('/opt/dagster/app/data/gold_output.csv', index=False)
    return df
