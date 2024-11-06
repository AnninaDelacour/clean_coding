from dagster import asset
import pandas as pd
from sklearn.model_selection import train_test_split
import os

data_folder = '/opt/dagster/app/data'

@asset
def test_train_split_data(context, silver_sorted):
    # Suchen des neuesten finalen Silver-Datasets
    csv_path = [f for f in os.listdir(data_folder) if f.startswith('silver_data_final_') and f.endswith('.csv')]
    latest_silver_file = max(csv_path, key=lambda x: os.path.getctime(os.path.join(data_folder, x)))
    latest_silver_file_path = os.path.join(data_folder, latest_silver_file)
    
    # Daten laden
    data = pd.read_csv(latest_silver_file_path)

    # Aufteilen in Features (X) und Zielvariable (y), hier Beispiel für Total_Price als Zielvariable
    X = data.drop(columns=['Total_Price'])
    y = data['Total_Price']

    # Train-Test-Split (80% Training, 20% Test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Ergebnisse als CSV speichern
    X_train.to_csv(os.path.join(data_folder, "X_train.csv"), index=False)
    X_test.to_csv(os.path.join(data_folder, "X_test.csv"), index=False)
    y_train.to_csv(os.path.join(data_folder, "y_train.csv"), index=False)
    y_test.to_csv(os.path.join(data_folder, "y_test.csv"), index=False)

    # Protokollnachricht
    context.log.info("Train-Test-Split erstellt und gespeichert.")

    pass
