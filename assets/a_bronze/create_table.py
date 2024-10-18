import sqlite3
import pandera as pa
from pandera import Column, DataFrameSchema

# Definiere das Schema für die CSV-Daten
transaction_schema = DataFrameSchema({
    "Transaction_ID": Column(pa.String),
    "Customer_ID": Column(pa.String),
    "Date_Time": Column(pa.DateTime),
    "Store_Location": Column(pa.String),
    "Product": Column(pa.String),
    "Brand": Column(pa.String),
    "Quantity": Column(pa.Int),
    "Total_Price": Column(pa.Float),
    "Payment_Method": Column(pa.String),
    "Ingested_Timestamp": Column(pa.DateTime)
})

def create_table_and_validate(df):
    # Validierung der Daten mit Pandera
    validated_df = transaction_schema.validate(df)

    conn = sqlite3.connect('/opt/dagster/app/transaction_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Transactions (
            Transaction_ID TEXT,
            Customer_ID TEXT,
            Date_Time TEXT,
            Store_Location TEXT,
            Product TEXT,
            Brand TEXT,
            Quantity INTEGER,
            Price_Per_Unit REAL,
            Total_Price REAL,
            Payment_Method TEXT,
            Ingested_Timestamp TEXT
        )
    ''')

    validated_df.to_sql('Transactions', conn, if_exists='append', index=False)

    conn.close()

    return validated_df
