import sqlite3
import pandera as pa
from pandera import Column, DataFrameSchema

# Definiere das Schema für die CSV-Daten
gold_ml_schema = DataFrameSchema({
    "Transaction_ID": Column(pa.Int),
    "Cashier_ID": Column(pa.String),
    "Date_Time": Column(pa.DateTime),
    "Store_Location": Column(pa.String),
    "Product": Column(pa.String),
    "Brand": Column(pa.String),
    "Quantity": Column(pa.Int),
    "Total_Price": Column(pa.Float),
    "Payment_Method": Column(pa.String),
    "Membership_ID": Column(pa.String),
    "Ingested_Timestamp": Column(pa.DateTime)
})

def create_table_and_validate(df):
    # Validierung der Daten mit Pandera
    validated_df = gold_ml_schema.validate(df)

    conn = sqlite3.connect('/opt/dagster/app/transaction_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Gold_ML (
            Transaction_ID INT,
            Cashier_ID TEXT,
            Date_Time TEXT,
            Store_Location TEXT,
            Product TEXT,
            Brand TEXT,
            Quantity INTEGER,
            Price_Per_Unit REAL,
            Total_Price REAL,
            Payment_Method TEXT,
            Membership_ID TEXT,
            Ingested_Timestamp TEXT
        )
    ''')

    validated_df.to_sql('Gold_ML', conn, if_exists='append', index=False)

    conn.close()

    return validated_df
