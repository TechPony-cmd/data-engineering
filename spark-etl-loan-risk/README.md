# Projet Title: Loan Risk ETL Pipeline with Apache Spark

This project uses Apache Spark to process loan application data, perform data cleaning, feature engineering, and calculate key risk metrics such as default rate, loan-to-income ratio, and average loan amount by credit score band. It simulates a distributed ETL pipeline you’d build in a real lending or fintech company.

## Project Structure

    ├── config.yaml #configuration file 
    ├── data/
    │   └── loan_data.csv # loan data
    ├── loan_etl.py #etl pipeline
    ├── run_loan_etl.sh #bash script to run etl
    ├── output/
    └── README.md # Project documentation 

## Features
- Load CSV loan data
- Clean data
- Calculate key metrics (approval rate)
- Save results as partitioned Parquet files

### Metrics Computed

This project calculates several key business metrics for evaluating loan applicant risk and affordability, grouped by credit score and financial tier:

1. **Approval Rate** by credit score, income, and loan purpose

## Run the Project
```bash
spark-submit loan_etl.py
```




