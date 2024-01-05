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
- containerization

### Overall ETL pipeline
Strava API --> Python --> BigQuery
- light transformation with Pandas
- orchestration through Google Cloud Services
- data storage through BigQuery 
- final data transformations with business logic with dbt
- Containerization via Docker
- BI dashboard via Looker Studio

### Folder structure
- configs : py file with API tokens, db user/password, ETL params
- src : source code
- tests : unit tests
