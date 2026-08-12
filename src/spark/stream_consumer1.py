from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek
)
from pyspark.sql.types import *


# -----------------------------------
# Create Spark Session
# -----------------------------------

spark = (
    SparkSession.builder
    .appName("SolarKafkaStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


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

    .option(
        "startingOffsets",
        "earliest"
    )

    .option(
        "failOnDataLoss",
        "false"
    )

    .load()

)


# -----------------------------------
# Parse Kafka JSON
# -----------------------------------

json_df = (

    kafka_df

    .selectExpr(
        "CAST(value AS STRING) AS json"
    )

    .select(
        from_json(
            col("json"),
            schema
        ).alias("data")
    )

    .select("data.*")

)


# -----------------------------------
# Ensure Correct Data Types
# -----------------------------------

final_df = (

    json_df

    .withColumn(
        "PLANT_ID",
        col("PLANT_ID").cast(IntegerType())
    )

    .withColumn(
        "DC_POWER",
        col("DC_POWER").cast(DoubleType())
    )

    .withColumn(
        "AC_POWER",
        col("AC_POWER").cast(DoubleType())
    )

    .withColumn(
        "DAILY_YIELD",
        col("DAILY_YIELD").cast(DoubleType())
    )

    .withColumn(
        "TOTAL_YIELD",
        col("TOTAL_YIELD").cast(DoubleType())
    )

    .withColumn(
        "AMBIENT_TEMPERATURE",
        col("AMBIENT_TEMPERATURE").cast(DoubleType())
    )

    .withColumn(
        "MODULE_TEMPERATURE",
        col("MODULE_TEMPERATURE").cast(DoubleType())
    )

    .withColumn(
        "IRRADIATION",
        col("IRRADIATION").cast(DoubleType())
    )

    .withColumn(
        "POWER_GENERATED",
        col("POWER_GENERATED").cast(DoubleType())
    )

)


# -----------------------------------
# Convert DATE_TIME to Timestamp
# -----------------------------------

final_df = final_df.withColumn(
    "DATE_TIME",
    to_timestamp(
        col("DATE_TIME"),
        "yyyy-MM-dd HH:mm:ss"
    )
)


# -----------------------------------
# Remove Null Values
# -----------------------------------

final_df = final_df.dropna(
    subset=[
        "DATE_TIME",
        "PLANT_ID",
        "DC_POWER",
        "AC_POWER",
        "POWER_GENERATED"
    ]
)


# -----------------------------------
# Remove Invalid Values
# -----------------------------------

final_df = final_df.filter(

    (col("DC_POWER") >= 0) &
    (col("AC_POWER") >= 0) &
    (col("DAILY_YIELD") >= 0) &
    (col("TOTAL_YIELD") >= 0) &
    (col("POWER_GENERATED") >= 0)

)


# -----------------------------------
# Generate Time Features
# -----------------------------------

final_df = (

    final_df

    .withColumn(
        "YEAR",
        year(col("DATE_TIME"))
    )

    .withColumn(
        "MONTH",
        month(col("DATE_TIME"))
    )

    .withColumn(
        "DAY",
        dayofmonth(col("DATE_TIME"))
    )

    .withColumn(
        "HOUR",
        hour(col("DATE_TIME"))
    )

    .withColumn(
        "DAY_OF_WEEK",
        dayofweek(col("DATE_TIME"))
    )

)


# -----------------------------------
# Write Clean Stream to HDFS
# -----------------------------------

query = (

    final_df

    .writeStream

    .format("parquet")

    .option(
        "path",
        "hdfs://localhost:9000/solar/data"
    )

    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/solar/checkpoint"
    )

    .outputMode("append")

    .start()

)


# -----------------------------------
# Start Streaming
# -----------------------------------

print("=" * 60)
print("Solar Kafka Streaming Started...")
print("Real-Time Preprocessing Enabled")
print("Writing Clean Streaming Data to HDFS...")
print("=" * 60)

query.awaitTermination()
