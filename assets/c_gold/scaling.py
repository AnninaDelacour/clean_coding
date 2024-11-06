from dagster import asset
import pandas as pd
from sklearn.preprocessing import StandardScaler
import os

data_folder = '/opt/dagster/app/data'

@asset
def scaling_data(context, one_hot_encoding_data):
    X_train = pd.read_csv(os.path.join(data_folder, "X_train_encoded.csv"))
    X_test = pd.read_csv(os.path.join(data_folder, "X_test_encoded.csv"))

    # Datums-Spalten identifizieren und herausnehmen
    datetime_columns = X_train.select_dtypes(include=['object']).columns[
        X_train.select_dtypes(include=['object']).apply(lambda col: pd.to_datetime(col, errors='coerce').notna().all())
    ]

    # Datums-Spalten aus den Datensätzen entfernen
    X_train_dates = X_train[datetime_columns]
    X_test_dates = X_test[datetime_columns]
    X_train = X_train.drop(columns=datetime_columns)
    X_test = X_test.drop(columns=datetime_columns)

    # Skalieren der verbleibenden numerischen Daten
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    X_train_scaled_df = pd.DataFrame(X_train_scaled, columns=X_train.columns)
    X_test_scaled_df = pd.DataFrame(X_test_scaled, columns=X_test.columns)

    # Datums-Spalten wieder hinzufügen
    X_train_final = pd.concat([X_train_scaled_df, X_train_dates.reset_index(drop=True)], axis=1)
    X_test_final = pd.concat([X_test_scaled_df, X_test_dates.reset_index(drop=True)], axis=1)

    X_train_final.to_csv(os.path.join(data_folder, "X_train_scaled.csv"), index=False)
    X_test_final.to_csv(os.path.join(data_folder, "X_test_scaled.csv"), index=False)

    context.log.info("Scaling durchgeführt und Ergebnisse gespeichert.")

    return {
        "X_train_scaled_path": os.path.join(data_folder, "X_train_scaled.csv"),
        "X_test_scaled_path": os.path.join(data_folder, "X_test_scaled.csv")
    }
