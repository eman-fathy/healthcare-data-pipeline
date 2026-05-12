# Healthcare Data Pipeline

End-to-end data pipeline for patient and hospital analytics, built on **Microsoft Azure / Fabric** as the final project for the **Digital Egypt Pioneers Initiative (DEPI)** — Data Engineering track.

The pipeline collects healthcare data from multiple sources, cleans and validates it, stores it in a cloud lakehouse, models it as a star schema, and exposes it through a Power BI dashboard for hospital managers and decision-makers.

---

## Architecture

```
Sources  →  Ingestion  →  Storage  →  Processing  →  Warehouse  →  Visualization
  CSV         Airflow      Data        Cleaning &      Fabric        Power BI
  APIs        + ADF        Lake        Transformation  Lakehouse     Dashboard
  Synthetic                Gen2        (PySpark)       (SQL views)
```

| Stage | Tool | What it does |
|---|---|---|
| Sources | CSV files, Public APIs, Python Faker | Raw healthcare data — patient visits, diagnoses, disease statistics |
| Ingestion | Apache Airflow / Azure Data Factory | Scheduled extraction and loading |
| Storage | Azure Data Lake Gen2 | Raw + cleaned data, kept in CSV and Parquet |
| Processing | PySpark | Cleans missing values, removes duplicates, normalizes columns |
| Warehouse | Microsoft Fabric SQL endpoint | Star schema: `fact_admissions` + `dim_*` views |
| Visualization | Power BI Desktop | Executive dashboard with KPIs, trends, breakdowns |

---

## Repository structure

```
.
├── healthcare_dataset.csv           # Source: hospital/patient records
├── api_disease_data.csv             # Source: public disease API data
├── fetch_api_data.py                # Pulls data from public healthcare APIs
├── transformation.py                # Pandas/PySpark cleaning logic
├── healthcare_dag.py                # Airflow orchestration DAG
├── healthcare_cleaned_csv/          # Cleaned output (CSV parts)
├── healthcare_cleaned_parquet/      # Cleaned output (Parquet parts)
├── fabric..sql                      # Warehouse schema: views, fact, dimensions
├── SQL script test.sql              # Ad-hoc queries / validation
├── HealthCare1.ipynb                # Exploration notebook
└── powerbi/
    └── healthcare_dashboard.pbix    # Power BI dashboard
```

---

## Data model (star schema)

The Fabric SQL endpoint exposes these views on top of `healthcare_silver`:

**Fact**
- `fact_admissions` — one row per admission, with keys to all dimensions

**Dimensions**
- `dim_patient` — patient_name, age, gender, age_group, blood_type, length_of_stay
- `dim_doctor` — doctor_name
- `dim_hospital` — hospital_name
- `dim_condition` — medical_condition
- `dim_insurance` — insurance_provider
- `dim_date` — full_date, year, month, day

**Reporting views**
- `VW_PATIENTS`, `VW_HOSPITALS`, `VW_CONDITIONS`, `VW_ADMISSIONS`
- `VW_EARLY_WARNING` — flags high-volume conditions
- `VW_EPIDEMIC_TRACKING` — cases by condition × season

To recreate the views in a fresh Fabric lakehouse, run `fabric..sql` against the SQL analytics endpoint.

---

## Power BI dashboard

The dashboard lives at [`powerbi/healthcare_dashboard.pbix`](powerbi/healthcare_dashboard.pbix).

**To open:**
1. Install Power BI Desktop (free, Windows).
2. Open the `.pbix` file.
3. If prompted to refresh credentials, sign in with the Microsoft account that has access to the Fabric lakehouse.

**Pages**
- **Executive Overview** — total admissions, revenue, average length of stay, admissions trend, top hospitals, insurance mix

**Key measures (DAX)**
- `Total Admissions` — count of admission rows
- `Total Revenue` — sum of billing_amount
- `Avg LOS` — average length_of_stay
- `Avg Billing` — revenue per admission
- `Unique Hospitals` / `Unique Patients` — distinct counts

---

## KPI categories covered

- **Operational** — bed occupancy, length of stay, doctor-to-patient ratio
- **Clinical** — most common diagnoses, treatment outcomes, mortality, disease trends
- **Financial** — cost per stay, revenue by department, insurance mix
- **Patient experience** — satisfaction, waiting time, no-show rate
- **Pipeline health** — execution time, data freshness, data quality

---

## Team

| Member | Role |
|---|---|
| **Eman Fathy** | Team Leader • Data Warehouse & SQL |
| Mohamed Essam | Data Ingestion & Pipeline Development |
| Eslam Abdelhady Mesbah | Data Processing & Transformation |
| Safaa Tolba | Data Warehouse & SQL |
| Ibrahim Abdel Salam | Dashboard & Reporting |

---

## Tools & technologies

Python · SQL · Apache Airflow · Azure Data Factory · Azure Data Lake Gen2 · Microsoft Fabric · PySpark · Power BI · Git
