from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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
# 16 Columns Dataset
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
# Read Data From Kafka
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
# Ensure Correct Data Types
# -----------------------------------

final_df = (

    json_df

    .withColumn(
        "PLANT_ID",
        col("PLANT_ID").cast(IntegerType())
    )

    .withColumn(
        "YEAR",
        col("YEAR").cast(IntegerType())
    )

    .withColumn(
        "MONTH",
        col("MONTH").cast(IntegerType())
    )

    .withColumn(
        "DAY",
        col("DAY").cast(IntegerType())
    )

    .withColumn(
        "HOUR",
        col("HOUR").cast(IntegerType())
    )

    .withColumn(
        "DAY_OF_WEEK",
        col("DAY_OF_WEEK").cast(IntegerType())
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
# Write Stream To HDFS Parquet
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


print("Solar Kafka Streaming Started...")


query.awaitTermination()
