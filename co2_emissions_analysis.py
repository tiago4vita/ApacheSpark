from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, row_number, desc, format_number
from pyspark.sql.window import Window
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

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
spark.sparkContext.setLogLevel("ERROR")

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

# Function to create line plots for emissions trends
def create_emissions_trend_plot(data_df, title, filename, limit=10):
    plt.figure(figsize=(15, 8))
    
    # Convert Spark DataFrame to Pandas for plotting
    pdf = data_df.limit(limit).toPandas()
    
    # Get all year columns
    year_columns = [str(year) for year in range(1960, 2019)]
    
    # Plot line for each country
    for _, row in pdf.iterrows():
        country = row['Country']
        emissions = [row[year] for year in year_columns]
        years = list(range(1960, 2019))
        
        plt.plot(years, emissions, marker='o', markersize=4, label=country)
    
    # Customize the plot
    plt.title(title, pad=20, fontsize=12, fontweight='bold')
    plt.xlabel('Year', fontsize=10)
    plt.ylabel('CO2 Emissions (metric tons per capita)', fontsize=10)
    
    # Add legend
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    
    # Rotate x-axis labels
    plt.xticks(years[::5], rotation=45)  # Show every 5th year
    
    # Add grid
    plt.grid(True, alpha=0.2)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(f'plots/{filename}.png', 
                dpi=300, 
                bbox_inches='tight',
                facecolor='white',
                edgecolor='none')
    plt.close()

# Modify the queries to get full data for top countries
# 1. Top increasing countries
top_increasing = data.select(
    "Country",
    col("1960").alias("1960_emissions"),
    col("2018").alias("2018_emissions")
) \
.filter(col("1960").isNotNull() & col("2018").isNotNull()) \
.withColumn("change_percentage", 
    ((col("2018_emissions") - col("1960_emissions")) / col("1960_emissions") * 100)) \
.filter(col("change_percentage") > 0) \
.orderBy(col("change_percentage").desc()) \
.select("Country") \
.limit(10)

# Join with original data to get all years for top countries
increasing_trends = data.join(top_increasing, "Country")

# Create trend plot for increasing emissions
create_emissions_trend_plot(
    increasing_trends,
    'CO2 Emissions Trends for Top 10 Countries with Highest Increase (1960-2018)',
    'increasing_emissions_trend'
)

# 2. Top decreasing countries
top_decreasing = data.select(
    "Country",
    col("1960").alias("1960_emissions"),
    col("2018").alias("2018_emissions")
) \
.filter(col("1960").isNotNull() & col("2018").isNotNull()) \
.withColumn("change_percentage", 
    ((col("2018_emissions") - col("1960_emissions")) / col("1960_emissions") * 100)) \
.filter(col("change_percentage") < 0) \
.orderBy(col("change_percentage").asc()) \
.select("Country") \
.limit(10)

# Join with original data to get all years for top countries
decreasing_trends = data.join(top_decreasing, "Country")

# Create trend plot for decreasing emissions
create_emissions_trend_plot(
    decreasing_trends,
    'CO2 Emissions Trends for Top 10 Countries with Highest Decrease (1960-2018)',
    'decreasing_emissions_trend'
)

# Add requirements to requirements.txt
with open('requirements.txt', 'w') as f:
    f.write('pyspark>=3.0.0\n')
    f.write('matplotlib>=3.0.0\n')
    f.write('seaborn>=0.11.0\n')
    f.write('pandas>=1.0.0\n')

# Close the Spark session
spark.stop()
