from pyspark.sql import SparkSession

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("SparkToMySQL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read Parquet data from HDFS
df = spark.read.parquet("hdfs://localhost:9000/solar/data")

# Select a few columns for testing
mysql_df = df.select(
    "DATE_TIME",
    "PLANT_ID",
    "POWER_GENERATED",
    "AMBIENT_TEMPERATURE",
    "IRRADIATION"
)

# MySQL Connection Properties
url = "jdbc:mysql://localhost:3306/solar_dashboard"
properties = {
    "user": "sparkuser",
    "password": "Spark@123",
    "driver": "com.mysql.cj.jdbc.Driver"
}
# Write DataFrame to MySQL
mysql_df.write \
    .mode("overwrite") \
    .jdbc(url=url, table="solar_generation", properties=properties)

print("Data successfully written to MySQL!")

spark.stop()
