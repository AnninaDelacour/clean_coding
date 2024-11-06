from dagster import asset
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
import os

data_folder = '/opt/dagster/app/data'

@asset
def feature_cross_data(context, scaling_data):
    X_train = pd.read_csv(os.path.join(data_folder, "X_train_scaled.csv"))
    X_test = pd.read_csv(os.path.join(data_folder, "X_test_scaled.csv"))

    # Identifizieren und Herausnehmen der DateTime-Spalten
    datetime_columns = X_train.select_dtypes(include=['object']).columns[
        X_train.select_dtypes(include=['object']).apply(lambda col: pd.to_datetime(col, errors='coerce').notna().all())
    ]

    # DateTime-Spalten entfernen
    X_train_dates = X_train[datetime_columns]
    X_test_dates = X_test[datetime_columns]
    X_train = X_train.drop(columns=datetime_columns)
    X_test = X_test.drop(columns=datetime_columns)

    poly = PolynomialFeatures(interaction_only=True, include_bias=False)

    # Feature Crossing nur auf numerischen Daten anwenden
    X_train_crossed = poly.fit_transform(X_train)
    X_test_crossed = poly.transform(X_test)

    crossed_feature_names = poly.get_feature_names_out(X_train.columns)

    X_train_crossed_df = pd.DataFrame(X_train_crossed, columns=crossed_feature_names)
    X_test_crossed_df = pd.DataFrame(X_test_crossed, columns=crossed_feature_names)

    X_train_final = pd.concat([X_train_crossed_df, X_train_dates.reset_index(drop=True)], axis=1)
    X_test_final = pd.concat([X_test_crossed_df, X_test_dates.reset_index(drop=True)], axis=1)

    X_train_final.to_csv(os.path.join(data_folder, "X_train_crossed.csv"), index=False)
    X_test_final.to_csv(os.path.join(data_folder, "X_test_crossed.csv"), index=False)

    context.log.info("Feature Cross durchgeführt und Ergebnisse gespeichert.")

    return {
        "X_train_crossed_path": os.path.join(data_folder, "X_train_crossed.csv"),
        "X_test_crossed_path": os.path.join(data_folder, "X_test_crossed.csv")
    }
