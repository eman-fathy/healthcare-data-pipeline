import requests
import json
import csv

# Fetch data from Open Disease API
url = "https://disease.sh/v3/covid-19/countries"
response = requests.get(url)
data = response.json()

# Save to CSV
with open("api_disease_data.csv", "w", newline="") as f:
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

print("Done! api_disease_data.csv created")