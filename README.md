# Strava ELT

![StravaELT](https://github.com/jairus-m/StravaELTPipeline/assets/114552516/cfd065cc-58fb-4fb2-9dd6-1fbb87787a09)

### Demo Project

The goal of this project is to implement the skeleton of a robust ELT pipeline. Things to consider are:
- version control
- development flow
- file project structure
- unit testing
- logging
- documentation
- virtual enviornments/dependency management
- orchestration
- general best practices for data engineering 
- containerization
- supporting downstream analytics/ML

### Overall ELT pipeline
Strava API --> Python --> BigQuery + dbt --> Tableau/ML in Jupyter Notebook
- light data transformation with Pandas
- orchestration through Google Cloud Services
- data storage through BigQuery 
- final data transformations with business logic through dbt
    - [dbt strava project](https://github.com/jairus-m/dbt-strava/tree/main)
- Containerization via Docker
- ELT job notifications sent through Slack 
- Downstream analytics supported by this pipeline
    - dashboard via [Tableau](https://public.tableau.com/app/profile/jairusmartinez/viz/PersonalStravaActivityData/Dashboard1)
    - cycling ML model via [Python/Sklearn](https://www.kaggle.com/code/jairusmartinez/cycling-energy-regression?trk=public_profile_project-button) 

### Deployment
- Python application is containerized and pushed to Google Cloud Artifact Registry
- Container is then deployed on Cloud Run Jobs at a set schedule
- Every midnight, the ELT pipeline is ran, checking for new data to upload to BigQuery
- At job completion, a Slack notification with job meta data and success status is sent

### Folder structure
- configs : .yml file with API tokens, db user/password, ELT params
- src : source code
- tests : unit tests

