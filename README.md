# Strava ETL

### Demo Project

The goal of this project is to try and implement the skeleton of a robust ETL pipeline. Things to consider are:
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
- light data transformation with Pandas
- orchestration through Google Cloud Services
- data storage through BigQuery 
- final data transformations with business logic through dbt
    - [dbt strava project](https://github.com/jairus-m/dbt-strava/tree/main)
- Containerization via Docker
- BI dashboard via Looker Studio
- ETL job notifications sent through Slack 

### Deployment
- Python application is containerized and pushed to Google Cloud Artifact Registry
- Container is then deployed on Cloud Run Jobs at a set schedule
- Every midnight, the ETL pipeline is ran, checking for new data to upload to BigQuery
- At job completion, a Slack notification with job meta data and success status is sent

### Folder structure
- configs : .yml file with API tokens, db user/password, ETL params
- src : source code
- tests : unit tests
