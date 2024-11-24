from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, month, year, desc, avg, round, to_date
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session
spark = SparkSession.builder \
    .appName("Dengue Analysis Curitiba") \
    .getOrCreate()

# Read CSV file
df = spark.read.csv("datasets/2024-11-12_dengue_dados_abertos.csv", 
                    header=True, 
                    sep=";",
                    encoding="latin1")

# Register the DataFrame as a temp view for SQL queries
df.createOrReplaceTempView("dengue_cases")

logger.info("=== Dengue Cases Analysis in Curitiba ===")

# 1. Cases by Neighborhood (Top 10)
logger.info("\n1. Top 10 Neighborhoods with Most Cases:")
neighborhood_cases = df.groupBy("BAIRRO DE RESIDÊNCIA") \
    .agg(count("*").alias("total_cases")) \
    .orderBy(desc("total_cases")) \
    .limit(10)
neighborhood_cases.show(truncate=False)

# 2. Gender Analysis with Percentages
logger.info("\n2. Cases by Gender with Percentages:")
total_cases = df.count()
gender_cases = df.groupBy("SEXO") \
    .agg(
        count("*").alias("total_cases"),
        round((count("*") * 100 / total_cases), 2).alias("percentage")
    ) \
    .orderBy(desc("total_cases"))
gender_cases.show()

# 3. Infection Type Analysis
logger.info("\n3. Cases by Infection Type with Percentages:")
infection_type = df.groupBy("LOCAL DE INFECÇÃO") \
    .agg(
        count("*").alias("total_cases"),
        round((count("*") * 100 / total_cases), 2).alias("percentage")
    ) \
    .orderBy(desc("total_cases"))
infection_type.show(truncate=False)

# 4. Age Group Analysis
logger.info("\n4. Cases by Age Group:")
df_with_age = df.withColumn("age_group", 
    when(col("IDADE (anos)") < 12, "Children (0-11)")
    .when(col("IDADE (anos)") < 18, "Adolescent (12-17)")
    .when(col("IDADE (anos)") < 60, "Adult (18-59)")
    .otherwise("Elderly (60+)"))

age_cases = df_with_age.groupBy("age_group") \
    .agg(
        count("*").alias("total_cases"),
        round(avg("IDADE (anos)"), 2).alias("avg_age"),
        round((count("*") * 100 / total_cases), 2).alias("percentage")
    ) \
    .orderBy(desc("total_cases"))
age_cases.show()

# 5. Monthly Distribution based on First Symptoms Date
logger.info("\n5. Monthly Distribution (by First Symptoms Date):")
monthly_cases = df.withColumn("month", 
    month(to_date(col("DATA DOS PRIMEIROS SINTOMAS"), "dd/MM/yyyy"))) \
    .groupBy("month") \
    .agg(count("*").alias("total_cases")) \
    .orderBy("month")

# Add month names for better readability
monthly_cases = monthly_cases.withColumn("month_name",
    when(col("month") == 1, "Janeiro")
    .when(col("month") == 2, "Fevereiro")
    .when(col("month") == 3, "Março")
    .when(col("month") == 4, "Abril")
    .when(col("month") == 5, "Maio")
    .when(col("month") == 6, "Junho")
    .when(col("month") == 7, "Julho")
    .when(col("month") == 8, "Agosto")
    .when(col("month") == 9, "Setembro")
    .when(col("month") == 10, "Outubro")
    .when(col("month") == 11, "Novembro")
    .when(col("month") == 12, "Dezembro")) \
    .select("month", "month_name", "total_cases") \
    .orderBy("month")

monthly_cases.show(truncate=False)

# 6. Confirmation Criterion Analysis
logger.info("\n6. Cases by Confirmation Criterion:")
confirmation_cases = df.groupBy("CRITÉRIO DE CONFIRMAÇÃO") \
    .agg(
        count("*").alias("total_cases"),
        round((count("*") * 100 / total_cases), 2).alias("percentage")
    ) \
    .orderBy(desc("total_cases"))
confirmation_cases.show(truncate=False)

# Stop Spark session
spark.stop()
