# src file structure

### main.py
- contains the main entry point for executing the ETL pipeline
    - __CLI command to run ETL job__: ```python path/main.py configs/dev_configs.yml```
    - main function initializes all the needed connections, parses the config YAML file, then runs the Strava_ETL.load() method
    to execute 
    - Slack notifications are enabled within this main function

### transformers
- strava_etl module
    - Strava_ETL class lives here
        - methods:
             - Strava_ETL.extract()
             - Strava_ETL.transform()
             - Strava_ETL.load()

### commons
- connectors module
    - StravaAPI connector class
        - methods:
            - StravaAPI.get_header()
            - StravaAPI.get_dataset()
    - BigQuery Connector class
        - methods:
            - BigQuery.create_tableset()
            - BigQuery.upload_dataset()
            - BigQuery.newest_data()
            - BigQuery.append_to_table()
            - BigQuery.table_exists()
            - BigQuery.query_table()
- slack_notifications module
    - SlackNotifications class
        - methods:
            - SlackNotifications.send_custom_message()
            - SlackNotifications.timing_message()
- utils module
    - UnitConversion class
        - methods:
            - UnitConversion.sec_to_min()
            - UnitConversion.meters_to_miles()
            - UnitConversion.meters_to_feet()
            - UnitConversion.mps_to_mph()