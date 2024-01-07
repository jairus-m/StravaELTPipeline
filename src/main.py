import argparse
import logging
import logging.config
import yaml
import time
import datetime
from commons.connectors import StravaAPIConnector, BigQueryConnector
from commons.slack_notifications import SlackNotifications
from transformers.strava_etl import StravaETL

def parse_config():
    """Parse YAML config file from CLI arg input"""
    parser = argparse.ArgumentParser(description='Run the Strava EL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))
    return config

def initialize_logging(config):
    """
    Initialize logging
    
    :param config: yaml config that is read in
    """
    log_config = config['logging']
    logging.config.dictConfig(log_config)

def initialize_slack(config):
    """
    Initialize SlackNotifications

    :param config: yaml config that is read in
    """
    channel = config['slack']['channel']
    token = config['slack']['token']
    slack = SlackNotifications(token, channel)
    return slack

def initialize_connectors(config):
    """
    Initialize the Strava and Bigquery connectors.

    :param config: yaml config that is read in
    """
    sac = StravaAPIConnector(
        config['strava_api']['STRAVA_AUTH_URL'],
        config['strava_api']['STRAVA_ACTIVITIES_URL'],
        config['strava_api']['STRAVA_PAYLOAD']
    )
    setl = StravaETL(
        sac.strava_auth_url,
        sac.strava_activities_url,
        sac.strava_payload,
        config['strava_api']['pages'],
        config['strava_api']['num_activities'],
        config['strava_api']['cols_to_drop']
    )
    bqc = BigQueryConnector(service_account_json=config['bigquery']['SERVICE_ACCOUNT_JSON'])
    return setl, bqc

def main():
    """Entry point for Strava ETL job"""
    try:
        start_time = time.time()
        
        config = parse_config()
        initialize_logging(config)
        slack = initialize_slack(config)
        setl, bqc = initialize_connectors(config)

        logger = logging.getLogger(__name__)
        logger.info('Starting ETL job.')

        project_name = config['bigquery']['project']
        dataset_name = config['bigquery']['dataset']
        table_name = config['bigquery']['table']

        # table/date col for sql_query
        table_id = ".".join([project_name, dataset_name, table_name])
        date_col_name = config['strava_api']['date_col_name']

        sql_query = f"""
        SELECT DISTINCT id, name, {date_col_name}
        FROM {table_id}
        ORDER BY {date_col_name} DESC
        LIMIT 50;
        """

        setl.load(bqc, project_name, dataset_name, table_name, sql_query, date_col_name)
        logger.info('ETL job complete.')

        duration = time.time() - start_time

        slack.timing_message(job='strava_etl', duration=duration)
        slack.send_custom_message('Job succeeded!')
    except Exception as e:
        logger.error('Error in main method: %s', e)
        slack.send_custom_message(f'Date: {datetime.datetime.now()}\nJob failed. Please check logs.')

if __name__ == '__main__':
    main()
