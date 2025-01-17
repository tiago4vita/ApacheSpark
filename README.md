# CO2 Emissions Analysis with Apache Spark

This project demonstrates the usage of Apache Spark and PySpark for analyzing CO2 emissions data from 1960 to 2018. It serves as a practical example of working with Spark's DataFrame API and performing data analysis on a real-world dataset.

## Project Overview

The main script `co2_emissions_analysis.py` showcases several key Spark/PySpark features:

- Creating and configuring a SparkSession
- Reading and processing CSV data
- Column manipulation and renaming
- Data filtering and transformation
- Calculating percentage changes
- Sorting and displaying results
- Using Spark SQL functions
- Formatting output

## Key Features

- Analysis of countries with increasing CO2 emissions (1960 vs 2018)
- Analysis of countries with decreasing CO2 emissions (1960 vs 2018)
- Interactive output with user prompts
- Integration with Spark UI for monitoring (accessible at http://localhost:4040)

## Requirements

- Python 3.x
- Apache Spark
- PySpark
- A CSV dataset containing CO2 emissions data (1960-2018)

## Usage

1. Ensure you have Apache Spark and PySpark installed
2. Place your CO2 emissions dataset in the `datasets` folder
3. Run the script using: `python co2_emissions_analysis.py`

This project serves as a practical example for those learning Apache Spark and PySpark, demonstrating real-world data analysis techniques and best practices.
##

# Dengue Cases Analysis with Apache Spark

This project demonstrates the usage of Apache Spark and PySpark for analyzing dengue cases data in Curitiba. The analysis spans various aspects such as neighborhood distribution, gender breakdown, infection type, age groups, and monthly trends. It serves as a practical example of utilizing Spark's DataFrame API for real-time data analysis on a public health dataset.

## Project Overview

The main script `dengue_cases_analysis.py` showcases several key Spark/PySpark features:

- Creating and configuring a SparkSession
- Reading and processing CSV data
- Data aggregation and transformation
- Filtering and grouping data by different criteria
- Calculating percentages and averages
- Sorting and displaying results
- Using Spark SQL functions
- Interactive logging output

## Key Features

- Analysis of top neighborhoods with the most dengue cases
- Gender-based case distribution with percentages
- Analysis of dengue cases by infection type
- Age group distribution with average age calculations
- Monthly distribution of cases based on the first symptoms date
- Breakdown of cases by confirmation criterion
- Interactive output with logging for better traceability

## Requirements

- Python 3.x
- Apache Spark
- PySpark
- A CSV dataset containing dengue cases data (from Curitiba)

## Usage

1. Ensure you have Apache Spark and PySpark installed.
2. Place your dengue cases dataset in the `datasets` folder.
3. Run the script using: `python dengue_cases_analysis.py`.

This project provides an insightful example of using Apache Spark and PySpark for public health data analysis, demonstrating real-world techniques and best practices for processing large datasets with Spark.

