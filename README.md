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
