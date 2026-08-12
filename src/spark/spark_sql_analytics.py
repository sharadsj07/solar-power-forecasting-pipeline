from pyspark.sql import SparkSession

# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Solar Spark SQL Analytics")
    .master("local[*]")
    .getOrCreate()
)

# Reduce unnecessary log messages
spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# Read Parquet Data from HDFS
# --------------------------------------------------
df = spark.read.parquet("hdfs://localhost:9000/solar/data")

# Register Temporary SQL View
df.createOrReplaceTempView("solar_generation")

print("=" * 60)
print("SOLAR POWER ANALYTICS USING SPARK SQL")
print("=" * 60)

# --------------------------------------------------
# Query 1 : Total Records
# --------------------------------------------------
print("\n1. Total Records")
spark.sql("""
SELECT COUNT(*) AS Total_Records
FROM solar_generation
""").show()

# --------------------------------------------------
# Query 2 : Average Power Generation
# --------------------------------------------------
print("\n2. Average Power Generation")
spark.sql("""
SELECT
    ROUND(AVG(POWER_GENERATED), 2) AS Average_Power
FROM solar_generation
""").show()

# --------------------------------------------------
# Query 3 : Monthly Average Power Generation
# --------------------------------------------------
print("\n3. Monthly Average Power Generation")
spark.sql("""
SELECT
    YEAR,
    MONTH,
    ROUND(AVG(POWER_GENERATED), 2) AS AVG_POWER
FROM solar_generation
GROUP BY YEAR, MONTH
ORDER BY YEAR, MONTH
""").show()

# --------------------------------------------------
# Query 4 : Daily Average Power Generation
# --------------------------------------------------
print("\n4. Daily Average Power Generation")
spark.sql("""
SELECT
    YEAR,
    MONTH,
    DAY,
    ROUND(AVG(POWER_GENERATED), 2) AS AVG_POWER
FROM solar_generation
GROUP BY YEAR, MONTH, DAY
ORDER BY YEAR, MONTH, DAY
""").show(31)

# --------------------------------------------------
# Query 5 : Hourly Average Power Generation
# --------------------------------------------------
print("\n5. Hourly Average Power Generation")
spark.sql("""
SELECT
    HOUR,
    ROUND(AVG(POWER_GENERATED), 2) AS AVG_POWER
FROM solar_generation
GROUP BY HOUR
ORDER BY HOUR
""").show(24)

# --------------------------------------------------
# Query 6 : Plant-wise Average Power
# --------------------------------------------------
print("\n6. Plant-wise Average Power Generation")
spark.sql("""
SELECT
    PLANT_ID,
    ROUND(AVG(POWER_GENERATED), 2) AS AVG_POWER
FROM solar_generation
GROUP BY PLANT_ID
ORDER BY PLANT_ID
""").show()

# --------------------------------------------------
# Query 7 : Day-wise Average Power
# --------------------------------------------------
print("\n7. Day of Week Average Power")
spark.sql("""
SELECT
    DAY_OF_WEEK,
    ROUND(AVG(POWER_GENERATED), 2) AS AVG_POWER
FROM solar_generation
GROUP BY DAY_OF_WEEK
ORDER BY DAY_OF_WEEK
""").show()

# --------------------------------------------------
# Query 8 : Temperature vs Power
# --------------------------------------------------
print("\n8. Temperature vs Power")
spark.sql("""
SELECT
    ROUND(AMBIENT_TEMPERATURE,1) AS TEMPERATURE,
    ROUND(AVG(POWER_GENERATED),2) AS AVG_POWER
FROM solar_generation
GROUP BY ROUND(AMBIENT_TEMPERATURE,1)
ORDER BY TEMPERATURE
""").show(20)

# --------------------------------------------------
# Query 9 : Irradiation vs Power
# --------------------------------------------------
print("\n9. Irradiation vs Power")
spark.sql("""
SELECT
    ROUND(IRRADIATION,2) AS IRRADIATION,
    ROUND(AVG(POWER_GENERATED),2) AS AVG_POWER
FROM solar_generation
GROUP BY ROUND(IRRADIATION,2)
ORDER BY IRRADIATION
""").show(20)

# --------------------------------------------------
# Query 10 : Top 10 Highest Power Generation Records
# --------------------------------------------------
print("\n10. Top 10 Highest Power Generation Records")
spark.sql("""
SELECT
    DATE_TIME,
    PLANT_ID,
    POWER_GENERATED
FROM solar_generation
ORDER BY POWER_GENERATED DESC
LIMIT 10
""").show(truncate=False)

# --------------------------------------------------
# Query 11 : Maximum Power Generated
# --------------------------------------------------
print("\n11. Maximum Power Generated")
spark.sql("""
SELECT
    MAX(POWER_GENERATED) AS MAX_POWER
FROM solar_generation
""").show()

# --------------------------------------------------
# Query 12 : Minimum Power Generated
# --------------------------------------------------
print("\n12. Minimum Power Generated")
spark.sql("""
SELECT
    MIN(POWER_GENERATED) AS MIN_POWER
FROM solar_generation
""").show()

# --------------------------------------------------
# Stop Spark
# --------------------------------------------------
spark.stop()
