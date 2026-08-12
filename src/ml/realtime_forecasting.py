import os
import joblib
import mysql.connector

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = os.path.expanduser("~/project_sj")

MODEL_PATH = os.path.join(
    PROJECT_ROOT,
    "models",
    "random_forest_solar_power.pkl"
)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "solar-data"

# MySQL
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "solar_dashboard"
MYSQL_USER = "sparkuser"
MYSQL_PASSWORD = "Spark@123"

# Streaming checkpoint
CHECKPOINT_PATH = (
    "hdfs://localhost:9000/solar/"
    "checkpoint_realtime_forecasting"
)


# ============================================================
# HEADER
# ============================================================

print("=" * 70)
print("SOLAR REAL-TIME RANDOM FOREST FORECASTING")
print("=" * 70)


# ============================================================
# LOAD RANDOM FOREST MODEL
# ============================================================

print("Loading Random Forest model...")

model = joblib.load(MODEL_PATH)

print("Model loaded successfully.")
print("Model type:", type(model))
print("Number of features:", model.n_features_in_)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SolarRealTimeRandomForestForecasting")
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# KAFKA MESSAGE SCHEMA
# ============================================================

schema = StructType([

    StructField(
        "DATE_TIME",
        StringType(),
        True
    ),

    StructField(
        "PLANT_ID",
        IntegerType(),
        True
    ),

    StructField(
        "SOURCE_KEY",
        StringType(),
        True
    ),

    StructField(
        "DC_POWER",
        DoubleType(),
        True
    ),

    StructField(
        "AC_POWER",
        DoubleType(),
        True
    ),

    StructField(
        "DAILY_YIELD",
        DoubleType(),
        True
    ),

    StructField(
        "TOTAL_YIELD",
        DoubleType(),
        True
    ),

    StructField(
        "AMBIENT_TEMPERATURE",
        DoubleType(),
        True
    ),

    StructField(
        "MODULE_TEMPERATURE",
        DoubleType(),
        True
    ),

    StructField(
        "IRRADIATION",
        DoubleType(),
        True
    ),

    StructField(
        "YEAR",
        IntegerType(),
        True
    ),

    StructField(
        "MONTH",
        IntegerType(),
        True
    ),

    StructField(
        "DAY",
        IntegerType(),
        True
    ),

    StructField(
        "HOUR",
        IntegerType(),
        True
    ),

    StructField(
        "DAY_OF_WEEK",
        IntegerType(),
        True
    ),

    StructField(
        "POWER_GENERATED",
        DoubleType(),
        True
    )
])


# ============================================================
# READ KAFKA STREAM
# ============================================================

print()
print("=" * 70)
print("Connecting to Kafka...")
print("Topic:", KAFKA_TOPIC)
print("=" * 70)

kafka_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )

    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    .option(
        "startingOffsets",
        "latest"
    )

    .option(
        "failOnDataLoss",
        "false"
    )

    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

parsed_df = (
    kafka_df

    .select(
        col("timestamp").alias(
            "KAFKA_TIMESTAMP"
        ),

        col("value")
        .cast("string")
        .alias("JSON_DATA")
    )

    .select(
        col("KAFKA_TIMESTAMP"),

        from_json(
            col("JSON_DATA"),
            schema
        ).alias("DATA")
    )

    .select(
        col("KAFKA_TIMESTAMP"),
        "DATA.*"
    )
)


# ============================================================
# REAL-TIME DATA VALIDATION
# ============================================================

clean_df = (
    parsed_df

    .filter(
        col("DATE_TIME").isNotNull()
    )

    .filter(
        col("PLANT_ID").isNotNull()
    )

    .filter(
        col("AC_POWER").isNotNull()
    )

    .filter(
        col("DC_POWER").isNotNull()
    )

    .filter(
        col("IRRADIATION").isNotNull()
    )

    .filter(
        col("POWER_GENERATED").isNotNull()
    )

    .filter(
        col("POWER_GENERATED") >= 0
    )

    .filter(
        col("IRRADIATION") >= 0
    )
)


# ============================================================
# REAL-TIME FORECASTING FUNCTION
# ============================================================

def predict_batch(batch_df, batch_id):

    print()
    print("=" * 70)
    print(
        f"REAL-TIME FORECASTING BATCH: {batch_id}"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # CHECK EMPTY BATCH
    # --------------------------------------------------------

    if batch_df.isEmpty():

        print("No records in this batch.")

        return


    # --------------------------------------------------------
    # COLLECT CURRENT MICRO-BATCH
    # --------------------------------------------------------

    rows = batch_df.collect()

    print(
        "Incoming records:",
        len(rows)
    )


    # --------------------------------------------------------
    # RANDOM FOREST FEATURE ORDER
    #
    # MUST EXACTLY MATCH TRAINING
    # --------------------------------------------------------

    feature_columns = [
    "DC_POWER",
    "AC_POWER",
    "DAILY_YIELD",
    "TOTAL_YIELD",
    "AMBIENT_TEMPERATURE",
    "MODULE_TEMPERATURE",
    "IRRADIATION",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "DAY_OF_WEEK"
    ]



    # --------------------------------------------------------
    # CREATE FEATURE MATRIX
    # --------------------------------------------------------

    X = []

    valid_rows = []

    for row in rows:

        try:

            features = [

                float(
                    row[column]
                )

                for column in feature_columns
            ]

            X.append(features)

            valid_rows.append(row)

        except (
            TypeError,
            ValueError
        ):

            print(
                "Skipping invalid record."
            )


    # --------------------------------------------------------
    # CHECK VALID RECORDS
    # --------------------------------------------------------

    if not X:

        print(
            "No valid records available "
            "for prediction."
        )

        return


    # --------------------------------------------------------
    # RANDOM FOREST PREDICTION
    # --------------------------------------------------------

    predictions = model.predict(X)

    print(
        "Predictions generated:",
        len(predictions)
    )


    # --------------------------------------------------------
    # CURRENT PREDICTION TIME
    # --------------------------------------------------------

    prediction_time = datetime.now()


    # --------------------------------------------------------
    # MYSQL CONNECTION
    # --------------------------------------------------------

    try:

        connection = mysql.connector.connect(

            host=MYSQL_HOST,

            port=MYSQL_PORT,

            database=MYSQL_DATABASE,

            user=MYSQL_USER,

            password=MYSQL_PASSWORD
        )

        cursor = connection.cursor()

        print(
            "MySQL connection established."
        )


    except Exception as e:

        print(
            "MySQL connection failed:"
        )

        print(e)

        return


    # --------------------------------------------------------
    # INSERT QUERY
    # --------------------------------------------------------

    insert_query = """

        INSERT INTO realtime_forecasts

        (
            EVENT_TIME,
            PREDICTION_TIME,
            PLANT_ID,
            SOURCE_KEY,
            ACTUAL_POWER,
            PREDICTED_POWER,
            IRRADIATION
        )

        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )

    """


    # --------------------------------------------------------
    # PREPARE MYSQL RECORDS
    # --------------------------------------------------------

    mysql_rows = []


    for row, prediction in zip(
        valid_rows,
        predictions
    ):

        # --------------------------------------------
        # Convert original dataset timestamp
        # --------------------------------------------

        event_time = None

        try:

            event_time = datetime.strptime(

                row["DATE_TIME"],

                "%Y-%m-%d %H:%M:%S"
            )

        except Exception:

            print(
                "Invalid EVENT_TIME:",
                row["DATE_TIME"]
            )


        # --------------------------------------------
        # Append record
        # --------------------------------------------

        mysql_rows.append(

            (
                event_time,

                prediction_time,

                int(
                    row["PLANT_ID"]
                ),

                row["SOURCE_KEY"],

                float(
                    row["POWER_GENERATED"]
                ),

                float(
                    prediction
                ),

                float(
                    row["IRRADIATION"]
                )
            )
        )


    # ========================================================
    # WRITE TO MYSQL
    # ========================================================

    try:

        cursor.executemany(
            insert_query,
            mysql_rows
        )

        connection.commit()

        print()
        print(
            f"Batch {batch_id} "
            "written to MySQL successfully."
        )

        print(
            "Records inserted:",
            len(mysql_rows)
        )


    except Exception as e:

        connection.rollback()

        print()
        print(
            "ERROR writing predictions "
            "to MySQL:"
        )

        print(e)

        cursor.close()
        connection.close()

        return


    # ========================================================
    # DISPLAY SAMPLE PREDICTIONS
    # ========================================================

    print()
    print(
        "Sample Real-Time Predictions:"
    )

    print("-" * 70)


    for record in mysql_rows[:10]:

        print(

            "EVENT_TIME:",
            record[0],

            "| PREDICTION_TIME:",
            record[1],

            "| ACTUAL:",
            round(
                record[4],
                4
            ),

            "| PREDICTED:",
            round(
                record[5],
                4
            ),

            "| IRRADIATION:",
            round(
                record[6],
                6
            )
        )


    # ========================================================
    # CLOSE MYSQL
    # ========================================================

    cursor.close()

    connection.close()

    print()
    print(
        f"Batch {batch_id} "
        "completed successfully."
    )


# ============================================================
# START STREAMING QUERY
# ============================================================

query = (

    clean_df

    .writeStream

    .foreachBatch(
        predict_batch
    )

    .outputMode(
        "append"
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .trigger(
        processingTime="10 seconds"
    )

    .start()
)


# ============================================================
# START MESSAGE
# ============================================================

print()
print("=" * 70)
print(
    "REAL-TIME RANDOM FOREST FORECASTING STARTED"
)
print(
    "Prediction interval: 10 seconds"
)
print(
    "Output table: realtime_forecasts"
)
print("=" * 70)


# ============================================================
# KEEP STREAM RUNNING
# ============================================================

query.awaitTermination()
