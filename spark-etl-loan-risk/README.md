# Projet Title: Loan Risk ETL Pipeline with Apache Spark

This project uses Apache Spark to process loan application data, perform data cleaning, feature engineering, and calculate key risk metrics such as default rate, loan-to-income ratio, and average loan amount by credit score band. It simulates a distributed ETL pipeline you’d build in a real lending or fintech company.

## Project Structure

    ├── config.yaml #configuration file 
    ├── data/
    │   └── loan_data.csv # loan data
    ├── etl.py #etl pipeline
    ├── run_etl.sh #bash script to run etl
    ├── output/
    │   └── non-aggregated/ # non-aggregated loan data
    └── README.md # Project documentation 

## Features
1. Load CSV loan data
2. Clean and Normalize 
3. Feature Engineering
4. Quality Checks
5. Save results as partitioned Parquet files

## Run the Project
```bash
spark-submit loan_etl.py
```




