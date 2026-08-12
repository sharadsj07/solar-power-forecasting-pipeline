from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    avg,
    max,
    min
)
from pyspark.sql.types import *

# -----------------------------------
# Create Spark Session
# -----------------------------------

spark = (
    SparkSession.builder
    .appName("SolarRealTimeWindowAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Solar Real-Time Window Analytics Started...")
print("10-second window analytics will be written to MySQL...")
print("=" * 60)


# -----------------------------------
# Kafka JSON Schema
# -----------------------------------

schema = StructType([

    StructField("DATE_TIME", StringType(), True),
    StructField("PLANT_ID", IntegerType(), True),
    StructField("SOURCE_KEY", StringType(), True),

    StructField("DC_POWER", DoubleType(), True),
    StructField("AC_POWER", DoubleType(), True),
    StructField("DAILY_YIELD", DoubleType(), True),
    StructField("TOTAL_YIELD", DoubleType(), True),

    StructField("AMBIENT_TEMPERATURE", DoubleType(), True),
    StructField("MODULE_TEMPERATURE", DoubleType(), True),
    StructField("IRRADIATION", DoubleType(), True),

    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("HOUR", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),

    StructField("POWER_GENERATED", DoubleType(), True)
])


# -----------------------------------
# Read Stream From Kafka
# -----------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )

    .option(
        "subscribe",
        "solar-data"
    )

    # Only process records arriving after
    # this streaming query starts
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


# -----------------------------------
# Capture Kafka Arrival Timestamp
# -----------------------------------

json_df = (
    kafka_df

    .select(
        col("timestamp").alias("STREAM_TIMESTAMP"),
        col("value").cast("string").alias("json")
    )

    .select(
        from_json(
            col("json"),
            schema
        ).alias("data"),
        col("STREAM_TIMESTAMP")
    )

    .select(
        "STREAM_TIMESTAMP",
        "data.*"
    )
)


# -----------------------------------
# Convert Original DATE_TIME
# -----------------------------------

final_df = (
    json_df

    .withColumn(
        "DATE_TIME",
        to_timestamp(
            col("DATE_TIME"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)


# -----------------------------------
# Real-Time Data Cleaning
# -----------------------------------

clean_df = (
    final_df

    .dropna(
        subset=[
            "DATE_TIME",
            "PLANT_ID",
            "POWER_GENERATED"
        ]
    )

    .filter(
        (col("POWER_GENERATED") >= 0) &
        (col("IRRADIATION") >= 0)
    )
)


# -----------------------------------
# 10-Second Real-Time Window
# -----------------------------------

windowed_df = (

    clean_df

    # Watermark allows Spark to finalize
    # completed event-time windows
    .withWatermark(
        "STREAM_TIMESTAMP",
        "10 seconds"
    )

    .groupBy(

        window(
            col("STREAM_TIMESTAMP"),
            "10 seconds"
        )

    )

    .agg(

        count("*").alias(
            "TOTAL_RECORDS"
        ),

        avg("POWER_GENERATED").alias(
            "AVG_POWER"
        ),

        max("POWER_GENERATED").alias(
            "MAX_POWER"
        ),

        min("POWER_GENERATED").alias(
            "MIN_POWER"
        ),

        avg("IRRADIATION").alias(
            "AVG_IRRADIATION"
        )
    )

    .select(

        col("window.start").alias(
            "WINDOW_START"
        ),

        col("window.end").alias(
            "WINDOW_END"
        ),

        col("TOTAL_RECORDS"),

        col("AVG_POWER"),

        col("MAX_POWER"),

        col("MIN_POWER"),

        col("AVG_IRRADIATION")
    )
)

# -----------------------------------
# MySQL Configuration
# -----------------------------------

mysql_url = (
    "jdbc:mysql://localhost:3306/solar_dashboard"
)

mysql_properties = {

    "user": "sparkuser",

    "password": "Spark@123",

    "driver": "com.mysql.cj.jdbc.Driver"
}


# -----------------------------------
# Write Each Window To MySQL
# -----------------------------------

def write_to_mysql(
    batch_df,
    batch_id
):

    print("=" * 60)

    print(
        f"Processing Window Batch ID: {batch_id}"
    )

    batch_df.show(
        truncate=False
    )

    (
        batch_df

        .write

        .mode("append")

        .jdbc(
            url=mysql_url,

            table="realtime_window_analytics",

            properties=mysql_properties
        )
    )

    print(
        f"Window Batch {batch_id} "
        f"written to MySQL"
    )

    print("=" * 60)


# -----------------------------------
# Start Streaming Query
# -----------------------------------

query = (

    windowed_df

    .writeStream

    .outputMode("append")

    .foreachBatch(
        write_to_mysql
    )

    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/solar/checkpoint_realtime_window"
    )

    .start()
)


# -----------------------------------
# Keep Streaming Application Running
# -----------------------------------

query.awaitTermination()
