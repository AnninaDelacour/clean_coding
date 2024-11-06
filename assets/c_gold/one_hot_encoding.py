from dagster import asset
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import os

data_folder = '/opt/dagster/app/data'

@asset
def one_hot_encoding_data(context, test_train_split_data):
    X_train = pd.read_csv(os.path.join(data_folder, "X_train.csv"))
    X_test = pd.read_csv(os.path.join(data_folder, "X_test.csv"))

    categorical_features = ['Store_Location', 'Product', 'Brand', 'Payment_Method', 'Cashier_ID', 'Membership_ID']

    # Initialisieren des OneHotEncoders
    encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)

    # One Hot Encoding auf die Trainingsdaten anwenden und dann auf die Testdaten übertragen
    X_train_encoded = encoder.fit_transform(X_train[categorical_features])
    X_test_encoded = encoder.transform(X_test[categorical_features])

    X_train_encoded = pd.DataFrame(X_train_encoded, columns=encoder.get_feature_names_out(categorical_features))
    X_test_encoded = pd.DataFrame(X_test_encoded, columns=encoder.get_feature_names_out(categorical_features))

    # Numerische Daten und kodierte Kategorien wieder zusammenführen
    X_train_final = pd.concat([X_train.drop(columns=categorical_features).reset_index(drop=True), X_train_encoded.reset_index(drop=True)], axis=1)
    X_test_final = pd.concat([X_test.drop(columns=categorical_features).reset_index(drop=True), X_test_encoded.reset_index(drop=True)], axis=1)

    X_train_final.to_csv(os.path.join(data_folder, "X_train_encoded.csv"), index=False)
    X_test_final.to_csv(os.path.join(data_folder, "X_test_encoded.csv"), index=False)

    context.log.info("One Hot Encoding durchgeführt und Ergebnisse gespeichert.")

    return {
        "X_train_path": os.path.join(data_folder, "X_train.csv"),
        "X_test_path": os.path.join(data_folder, "X_test.csv"),
        "y_train_path": os.path.join(data_folder, "y_train.csv"),
        "y_test_path": os.path.join(data_folder, "y_test.csv")
    }
