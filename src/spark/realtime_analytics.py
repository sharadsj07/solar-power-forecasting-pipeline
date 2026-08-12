from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
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
    .appName("SolarRealTimeAnalytics")
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
# Convert Kafka JSON Message
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
# Convert DATE_TIME to Timestamp
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
# Remove Invalid Records
# -----------------------------------

final_df = (

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
# Real-Time Analytics
# -----------------------------------

analytics_df = (

    final_df

    .groupBy()

    .agg(

        count("*").alias("TOTAL_RECORDS"),

        avg("POWER_GENERATED").alias("AVG_POWER"),

        max("POWER_GENERATED").alias("MAX_POWER"),

        min("POWER_GENERATED").alias("MIN_POWER"),

        avg("IRRADIATION").alias("AVG_IRRADIATION")

    )
)


# -----------------------------------
# Display Real-Time Analytics
# -----------------------------------

analytics_query = (

    analytics_df

    .writeStream

    .format("console")

    .outputMode("complete")

    .option(
        "truncate",
        "false"
    )

    .start()
)


# -----------------------------------
# Start Analytics
# -----------------------------------

print("=" * 60)
print("Solar Real-Time Analytics Started...")
print("Calculating Live Power Metrics...")
print("=" * 60)

analytics_query.awaitTermination()
