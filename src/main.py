import argparse
import logging
import logging.config
import yaml
from nodes.utils import StravaAPIConnector, BigQueryConnector
from transformers.extract_load import StravaEL

def main():
    """
    Entry point to run the Strava EL job.
    """
    # parsing YML file
    parser = argparse.ArgumentParser(description='Run the Strava EL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()

    config = yaml.safe_load(open(args.config))

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)

    # initialize StravaAPIConnector class
    STRAVA_AUTH_URL = config['strava_api']['STRAVA_AUTH_URL']
    STRAVA_ACTIVITIES_URL = config['strava_api']['STRAVA_ACTIVITIES_URL']
    STRAVA_PAYLOAD = config['strava_api']['STRAVA_PAYLOAD']

    sac = StravaAPIConnector(STRAVA_AUTH_URL, STRAVA_ACTIVITIES_URL, STRAVA_PAYLOAD)

    # intialize StravaEL class
    sel = StravaEL(sac.strava_auth_url, sac.strava_activities_url, sac.strava_payload, 2, 200)

    # initlaize BigQueryConnector class
    SERVICE_ACCOUNT_JSON = config['bigquery']['SERVICE_ACCOUNT_JSON']
    bqc = BigQueryConnector(service_account_json=SERVICE_ACCOUNT_JSON)

    # run
    logger = logger = logging.getLogger(__name__)
    logger.info('Starting EL job.')

    project_name = config['bigquery']['project']
    dataset_name = config['bigquery']['dataset']
    table_name = config['bigquery']['table']

    table_id = ".".join([project_name, dataset_name, table_name])

    sql_query = f"""
    SELECT DISTINCT id, name, start_date
    FROM {table_id}
    ORDER BY start_date DESC
    LIMIT 50;
    """

    if bqc.table_exists(dataset_name, table_name) is True: 
        df_new = bqc.newest_data(sel.extract(), sql_query)
        if len(df_new) > 0:
            logger.info(f'Appending new data... {len(df_new)} new activities.')
            bqc.append_to_table(table_id, df_new)
        else:
            logger.info('Data up to date!')
    else:
        logger.info('Table not found. Batch loading last 200 activities.')
        sel.load(bqc, table_id, 200)

if __name__ == '__main__':
    main()
