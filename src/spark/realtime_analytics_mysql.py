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
    .appName("SolarRealTimeAnalyticsMySQL")
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
        "latest"
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
# Convert DATE_TIME
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
# Clean Data
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
# MySQL Configuration
# -----------------------------------

mysql_url = "jdbc:mysql://localhost:3306/solar_dashboard"

mysql_properties = {
    "user": "sparkuser",
    "password": "Spark@123",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# -----------------------------------
# Write Each Micro-Batch To MySQL
# -----------------------------------

def write_to_mysql(batch_df, batch_id):

    print("=" * 60)
    print(f"Processing Batch ID: {batch_id}")

    batch_df.show(truncate=False)

    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=mysql_url,
            table="realtime_analytics",
            properties=mysql_properties
        )
    )

    print(f"Batch {batch_id} written to MySQL")
    print("=" * 60)


# -----------------------------------
# Start Streaming Query
# -----------------------------------

query = (

    analytics_df

    .writeStream

    .outputMode("complete")

    .foreachBatch(write_to_mysql)

    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/solar/checkpoint_realtime_analytics"
    )

    .start()
)


# -----------------------------------
# Start Application
# -----------------------------------

print("=" * 60)
print("Solar Real-Time Analytics Started...")
print("Analytics results will be written to MySQL...")
print("=" * 60)

query.awaitTermination()
