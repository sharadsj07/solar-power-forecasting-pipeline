from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    avg,
    max,
    min,
    sum as spark_sum
)
from pyspark.sql.types import *

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("SolarRealTimePreprocessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("SOLAR REAL-TIME PREPROCESSING + ANALYTICS")
print("=" * 70)


# ============================================================
# Kafka JSON Schema
# ============================================================

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


# ============================================================
# Read Kafka Stream
# ============================================================

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
# Parse JSON
# ============================================================

parsed_df = (

    kafka_df

    .select(
        col("timestamp").alias("STREAM_TIMESTAMP"),
        col("value").cast("string").alias("json")
    )

    .select(
        col("STREAM_TIMESTAMP"),

        from_json(
            col("json"),
            schema
        ).alias("data")
    )

    .select(
        "STREAM_TIMESTAMP",
        "data.*"
    )
)


# ============================================================
# Timestamp Conversion
# ============================================================

parsed_df = (

    parsed_df

    .withColumn(
        "DATE_TIME",

        to_timestamp(
            col("DATE_TIME"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)


# ============================================================
# REAL-TIME PREPROCESSING
# ============================================================

clean_df = (

    parsed_df

    # Remove records with required fields missing
    .dropna(
        subset=[
            "DATE_TIME",
            "PLANT_ID",
            "POWER_GENERATED"
        ]
    )

    # Remove invalid physical values
    .filter(
        (col("POWER_GENERATED") >= 0) &
        (col("IRRADIATION") >= 0)
    )
)


# ============================================================
# 10-SECOND WINDOW ANALYTICS
# ============================================================

windowed_df = (

    clean_df

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
            "CLEAN_RECORDS"
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

        col("CLEAN_RECORDS"),

        col("AVG_POWER"),

        col("MAX_POWER"),

        col("MIN_POWER"),

        col("AVG_IRRADIATION")
    )
)


# ============================================================
# MySQL Configuration
# ============================================================

mysql_url = (
    "jdbc:mysql://localhost:3306/solar_dashboard"
)

mysql_properties = {

    "user": "sparkuser",

    "password": "Spark@123",

    "driver": "com.mysql.cj.jdbc.Driver"
}


# ============================================================
# Write To MySQL
# ============================================================

def write_to_mysql(batch_df, batch_id):

    print("=" * 70)

    print(
        f"REAL-TIME PREPROCESSING BATCH: {batch_id}"
    )

    print(
        "Clean records after validation:"
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

            table="realtime_preprocessing_analytics",

            properties=mysql_properties
        )
    )

    print(
        f"Batch {batch_id} written to MySQL"
    )

    print("=" * 70)


# ============================================================
# Start Streaming
# ============================================================

query = (

    windowed_df

    .writeStream

    .outputMode("append")

    .foreachBatch(
        write_to_mysql
    )

    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/solar/checkpoint_realtime_preprocessing"
    )

    .start()
)


# ============================================================
# Keep Application Running
# ============================================================

query.awaitTermination()
