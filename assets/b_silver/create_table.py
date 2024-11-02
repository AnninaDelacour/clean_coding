import sqlite3
import pandera as pa
from pandera import Column, DataFrameSchema

# Definiere das Schema für die CSV-Daten
silver_schema = DataFrameSchema({
    "Transaction_ID": Column(pa.Int),
    "Cashier_ID": Column(pa.String, nullable=True),
    "Date_Time": Column(pa.DateTime, nullable=True),
    "Store_Location": Column(pa.String, nullable=True),
    "Product": Column(pa.String, nullable=True),
    "Brand": Column(pa.String, nullable=True),
    "Quantity": Column(pa.Int, nullable=True),
    "Total_Price": Column(pa.Float, nullable=True),
    "Payment_Method": Column(pa.String, nullable=True),
    "Membership_ID": Column(pa.String, nullable=True),
    "Ingested_Timestamp": Column(pa.DateTime)
})

def create_table_and_validate(df):
    # Validierung der Daten mit Pandera
    validated_df = silver_schema.validate(df)

    conn = sqlite3.connect('/opt/dagster/app/transaction_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Silver (
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

    validated_df.to_sql('Silver', conn, if_exists='append', index=False)

    conn.close()

    return validated_df
