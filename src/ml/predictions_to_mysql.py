import pandas as pd
import mysql.connector


# ============================================================
# LOAD RANDOM FOREST PREDICTIONS INTO MYSQL
# ============================================================

print("=" * 70)
print("LOADING RANDOM FOREST PREDICTIONS INTO MYSQL")
print("=" * 70)


# ------------------------------------------------------------
# 1. Load predictions
# ------------------------------------------------------------

file_path = "data/processed/random_forest_predictions.csv"

df = pd.read_csv(file_path)

df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"])

print(f"Predictions loaded: {len(df)}")


# ------------------------------------------------------------
# 2. Connect to MySQL
# ------------------------------------------------------------

connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    database="solar_dashboard",
    user="sparkuser",
    password="Spark@123"
)

cursor = connection.cursor()

print("MySQL connection established.")


# ------------------------------------------------------------
# 3. Insert predictions
# ------------------------------------------------------------

insert_query = """
INSERT INTO solar_forecasts
(
    DATE_TIME,
    PLANT_ID,
    SOURCE_KEY,
    ACTUAL_POWER,
    PREDICTED_POWER
)
VALUES (%s, %s, %s, %s, %s)
"""

records = [
    (
        row.DATE_TIME.to_pydatetime(),
        int(row.PLANT_ID),
        row.SOURCE_KEY,
        float(row.ACTUAL_POWER),
        float(row.PREDICTED_POWER)
    )
    for row in df.itertuples(index=False)
]


cursor.executemany(insert_query, records)

connection.commit()

print(f"Inserted records: {cursor.rowcount}")


# ------------------------------------------------------------
# 4. Close connection
# ------------------------------------------------------------

cursor.close()
connection.close()

print("MySQL connection closed.")

print("=" * 70)
print("PREDICTIONS SUCCESSFULLY LOADED INTO MYSQL")
print("=" * 70)
