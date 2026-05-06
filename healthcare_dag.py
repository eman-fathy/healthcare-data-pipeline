from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import csv

# Task 1: Extract data from API
def extract_api_data():
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(url)
    data = response.json()
    with open("/tmp/api_disease_data.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["country", "cases", "deaths", "recovered", "active", "population"])
        for country in data:
            writer.writerow([
                country.get("country"),
                country.get("cases"),
                country.get("deaths"),
                country.get("recovered"),
                country.get("active"),
                country.get("population")
            ])
    print("API data extracted successfully!")

# Task 2: Validate data
def validate_data():
    with open("/tmp/api_disease_data.csv", "r") as f:
        rows = list(csv.reader(f))
    print(f"Total rows: {len(rows)}")
    print("Data validation passed!")

with DAG(
    dag_id="healthcare_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_api_data",
        python_callable=extract_api_data
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    extract >> validate