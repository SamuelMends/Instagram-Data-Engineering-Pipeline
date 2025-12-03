# Instagram Data Engineering Pipeline 🚀

**Status:** ✅ Completed

## 📖 Table of Contents  
- [About the project](#about-the-project)  
- [Technologies](#technologies)  
- [Repository Structure](#repository-structure)  
- [Pipeline Overview](#pipeline-overview)  
- [Setup & Run](#setup--run)  
- [Future Improvements](#future-improvements)  
- [Author / Contact](#author--contact)  

## About the project  
This project builds an end-to-end data engineering pipeline using Databricks and PySpark, based on Instagram post data.  
It processes raw data (Bronze), applies transformations and cleaning (Silver), and produces analytics-ready tables (Gold) — enabling social media analytics and insights generation.

## Technologies  
- Databricks  
- PySpark / Spark SQL  
- Delta Lake  
- Python 3.x  

## 📁 Repository Structure
- **Instagram-Data-Engineering-Pipeline**
  - `notebooks_clean/` — Cleaned notebooks / scripts
    - `0.1 Bronze.py`
    - `0.2 Silver.py`
    - `0.3 Gold.py`
  - `data/` — Instagram_Analytics.csv
  - `README.md` — This file
  - `LICENSE`



## Pipeline Overview  
- **Bronze**: raw data ingestion, normalization, and persistence as Delta table  
- **Silver**: data cleaning, type casting, date/time parsing, standardization  
- **Gold**: final analytics tables — ready for business insights  

Raw Data → Bronze (Delta Raw) → Silver (Cleaned) → Gold (Analytics)

## Setup & Run  
1. Clone the repository  
2. Import notebooks/scripts into Databricks  
3. Configure your Spark/Databricks environment  
4. Run pipeline in order: `Bronze → Silver → Gold`  
5. Check resulting tables in catalog / workspace  

## Future Improvements  
- Add automated tests / data quality checks  
- Parameterize pipeline (config file)  
- Schedule with orchestration (Airflow/Jobs)  
- Extend with more social media metrics / dashboards  

## Author  
Sam Mendes — https://www.linkedin.com/in/samuel-mendes1/  
