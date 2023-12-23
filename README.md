# Strava ETL

### Demo Project

The goal of this project is to try and implement the skeleton of a decent ETL pipeline. Things to consider are:
- version control
- development flow
- file project structure
- unit testing
- logging
- installable packages
- orchestration
- general best practives for data engineering 

### Overall ETL pipeline
Strava API --> Python --> BigQuery
- light transformation with Pandas
- orchestration with Apache Airflow / GitHub Actions
- datawarehouse to use is BigQuery 
- final data transformations with business logic with dbt

### Folder structure
configs : py file with API tokens, db user/password
docs : documentation 
logs : logging files
src : source code
test_data : raw, processed, test data
tests : unit tests