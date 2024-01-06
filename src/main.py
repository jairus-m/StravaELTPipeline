import argparse
import logging
import logging.config
import yaml
from nodes.utils import StravaAPIConnector, BigQueryConnector

from transformers.strava_etl import StravaETL

def parse_config():
    """
    Parse YML config file.
    """
    parser = argparse.ArgumentParser(description='Run the Strava EL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))

    return config

def logging_config(config):
    """
    Configure logging.
    """
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    return True


def main():
    """
    Entry point to run the Strava EL job.
    """
    config = parse_config()
    logging_config(config=config)

    # initialize StravaAPIConnector class 
    STRAVA_AUTH_URL = config['strava_api']['STRAVA_AUTH_URL']
    STRAVA_ACTIVITIES_URL = config['strava_api']['STRAVA_ACTIVITIES_URL']
    STRAVA_PAYLOAD = config['strava_api']['STRAVA_PAYLOAD']

    sac = StravaAPIConnector(STRAVA_AUTH_URL, STRAVA_ACTIVITIES_URL, STRAVA_PAYLOAD)

     # intialize StravaETL class
    pages = config['strava_api']['pages']
    num_activities = config['strava_api']['num_activities']
    cols_to_drop = config['strava_api']['cols_to_drop']

    setl = StravaETL(sac.strava_auth_url, sac.strava_activities_url, sac.strava_payload, pages, num_activities, cols_to_drop)

    # initlaize BigQueryConnector class
    SERVICE_ACCOUNT_JSON = config['bigquery']['SERVICE_ACCOUNT_JSON']

    bqc = BigQueryConnector(service_account_json=SERVICE_ACCOUNT_JSON)

    # logging for main()
    logger = logging.getLogger(__name__)
    logger.info('Starting ETL job.')

    # constants for StravaETL class and sql_query
    project_name = config['bigquery']['project']
    dataset_name = config['bigquery']['dataset']
    table_name = config['bigquery']['table']
    table_id = ".".join([project_name, dataset_name, table_name])
    date_col_name = config['strava_api']['date_col_name']

    sql_query = f"""
    SELECT DISTINCT id, name, {date_col_name}
    FROM {table_id}
    ORDER BY {date_col_name} DESC
    LIMIT 50;
    """

    # load updated data to BigQuery
    setl.load(bqc, project_name, dataset_name, table_name, sql_query, date_col_name)
    logger.info('ETL job complete.')

if __name__ == '__main__':
    main()
