from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, row_number, desc, format_number
from pyspark.sql.window import Window
import logging

# Configure logging to suppress Spark warnings
logging.getLogger("py4j").setLevel(logging.ERROR)

# Set Spark configurations to suppress warnings and enable UI
spark = SparkSession.builder \
    .appName("CO2 Emissions Analysis") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.sql.repl.eagerEval.enabled", "true") \
    .config("spark.log.level", "ERROR") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Add this line after creating the Spark session
print("Spark UI is running at http://localhost:4040")

# Suppress Spark logging
spark.sparkContext.setLogLevel("ERROR")s

# Load CSV file with the new schema
file_path = "datasets/CO2_Emissions_1960-2018.csv"
data = spark.read.csv(file_path, header=False, inferSchema=True)

# Rename columns: first column is Country, rest are years 1960-2018
years = list(range(1960, 2019))
new_columns = ["Country"] + [str(year) for year in years]
data = data.toDF(*new_columns)

# Dataset schema
print("\n=== Dataset schema ===")
data.printSchema()
input("Press ENTER to continue...")

# 1. Countries with increasing CO2 emissions (comparing 1960 vs 2018)
print("\n=== Countries with increasing emissions (comparing 1960 vs 2018) ===")
emissions_change = data.select(
    "Country",
    col("1960").alias("1960 (metric tons per capita)"),
    col("2018").alias("2018 (metric tons per capita)")
) \
.filter(col("1960").isNotNull() & col("2018").isNotNull()) \
.withColumn("change_percentage", 
    ((col("2018 (metric tons per capita)") - col("1960 (metric tons per capita)")) / 
     col("1960 (metric tons per capita)") * 100)) \
.filter(col("change_percentage") > 0) \
.orderBy(col("change_percentage").desc())

# Format the change_percentage to 2 decimal places
emissions_change = emissions_change \
    .withColumn("change_percentage", format_number("change_percentage", 2).alias("change (%)"))

emissions_change.show(10)
input("Press ENTER to continue...")

# 2. Countries with decreasing CO2 emissions
print("\n=== Countries with decreasing emissions (comparing 1960 vs 2018) ===")
emissions_change = data.select(
    "Country",
    col("1960").alias("1960 (metric tons per capita)"),
    col("2018").alias("2018 (metric tons per capita)")
) \
.filter(col("1960").isNotNull() & col("2018").isNotNull()) \
.withColumn("change_percentage", 
    ((col("2018 (metric tons per capita)") - col("1960 (metric tons per capita)")) / 
     col("1960 (metric tons per capita)") * 100)) \
.filter(col("change_percentage") < 0) \
.orderBy(col("change_percentage").asc())

emissions_change.show(10)
input("Press ENTER to continue...")

# Close the Spark session
spark.stop()
