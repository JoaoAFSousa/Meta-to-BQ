from datetime import datetime, timedelta, timezone
import asyncio
import logging

import pandas as pd
from google.cloud import bigquery
from meta_marketing import MetaClient
from google.api_core.exceptions import NotFound

logger = logging.getLogger(__name__)

def df_to_bq(
        table_id: str, 
        df: pd.DataFrame, 
        write_mode: str, 
        client: bigquery.Client
    ):
    '''Takes a dataframe and writes it in a BigQuery table.'''
    if write_mode == 'truncate':
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    elif write_mode == 'append':
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        )
    else:
        logger.error("Invalid write mode value. \nPlease insert 'truncate' or 'append'.")
    if df.shape[0] > 0:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
    else:
        logger.warning(f'{table_id} \n => Dataframe is empty, no data loaded in bigquery.')

def extract_account(
    ad_account_id: str, 
    meta_client: MetaClient, 
    tables: list,
    start: str,
    end: str = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
):
    supported_tables = {
        'campaigns': meta_client.df_from_campaigns,
        'adsets': meta_client.df_from_adsets,
        'ads': meta_client.df_from_ads,
        'insights_ads': meta_client.df_from_ad_insights,
        'monthly_insights_accounts': meta_client.df_from_monthly_insights_account,
        'monthly_insights_ads': meta_client.df_from_monthly_insights_ads,
        'monthly_insights_campaigns': meta_client.df_from_monthly_insights_campaigns,
        'monthly_insights_adsets': meta_client.df_from_monthly_insights_adsets,
        'adcreatives': meta_client.df_from_adcreatives
    }
    dict_tables = {}
    for table, method in supported_tables.items():
        if table in tables:
            try:
                df = (
                    method(ad_account_id=ad_account_id)
                    if 'insights' not in table
                    else method(start=start, end=end, ad_account_id=ad_account_id)
                )
                dict_tables.update({table: df})
                logger.info(f'Extracted table {table} from ad account {ad_account_id} successfully.')
            except Exception:
                logger.exception(f'Error loading table {table} from ad account {ad_account_id}')
                raise
    return dict_tables

# MULTIPLE ACCOUNTS EXTRACTION SUSPENDED IN LOAD JOB - REIMPLAMENTATION WILL BE EVALUATED
async def extract_account_async(
    ad_account_id: str, 
    meta_client, 
    start: str,
    end: str = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
):
    return await asyncio.to_thread(extract_account, ad_account_id, meta_client, start, end)

async def extract_accounts_async(
    ad_account_ids: str | list, 
    meta_client, 
    start: str,
    end: str = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')
):
    ad_account_ids = [ad_account_ids] if isinstance(ad_account_ids, str) else ad_account_ids
    extractions = [extract_account_async(id, meta_client, start, end) for id in ad_account_ids]
    extracted_data = await asyncio.gather(*extractions)

    campaigns_list, adsets_list, ads_list, insights_list = zip(*extracted_data)

    df_campaigns = pd.concat(campaigns_list, ignore_index=True)
    df_adsets = pd.concat(adsets_list, ignore_index=True)
    df_ads = pd.concat(ads_list, ignore_index=True)
    df_insights = pd.concat(insights_list, ignore_index=True)

    return df_campaigns, df_adsets, df_ads, df_insights

def load(
    ad_account_ids: str | list,
    meta_client: MetaClient,
    bq_client: bigquery.Client,
    bq_project_id: str,
    bq_dataset: str,
    start: str = datetime(datetime.now().year, datetime.now().month, 1).strftime('%Y%m%d'),
    end: str = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d'),
    write_mode: str = 'append',
    tables: list = ['campaigns', 'adsets', 'ads', 'insights_ads']
):
    'Loads data from a list of ad account into a BQ dataset.'
    # Creating dataset if it doesn't exist yet (it will be overwritten if exists)
    if write_mode not in ['append', 'truncate']:
        raise ValueError("Insert a valid write mode, 'append' or 'truncate'.")
    
    dataset_query = f'''CREATE SCHEMA IF NOT EXISTS `{bq_project_id}.{bq_dataset}`'''
    bq_client.query(dataset_query)

    dict_tables = extract_account(
        ad_account_id=ad_account_ids,
        meta_client=meta_client,
        start=start,
        end=end,
        tables=tables
    )
    for table, df in dict_tables.items():
        write_mode_internal = 'append'
        ad_accounts_str = "', '".join(ad_account_ids) if isinstance(ad_account_ids, list) else ad_account_ids
        if table.count('insights') == 1 and write_mode == 'append': # Deletes data to be overwritten
            count_query = f'''
                SELECT COUNT(*) AS data_count
                FROM {bq_project_id}.{bq_dataset}.{table}
                WHERE 
                    account_id IN ('{ad_accounts_str}')
                    AND date_start >= '{start}'
                    AND date_start <= '{end}'
            '''
        else:
            count_query = f'''
                SELECT COUNT(*) AS data_count
                FROM {bq_project_id}.{bq_dataset}.{table}
                WHERE 
                    account_id IN ('{ad_accounts_str}')
            '''

        del_query = count_query.replace('SELECT COUNT(*) AS data_count', 'DELETE')
        try:
            df_count = bq_client.query(count_query).to_dataframe()
            if df_count['data_count'][0] > 0:
                del_job = bq_client.query(del_query)
                del_job.result()
                logger.info(f'{df_count['data_count'][0]} rows to be deleted from {bq_project_id}.{bq_dataset}.{table}, account_ids: {ad_accounts_str}, start: {start} for update.')
        except NotFound:  # In case the table doesn't exist yet.
            pass
        
        df_to_bq(
            table_id=f'{bq_project_id}.{bq_dataset}.{table}', 
            df=df, 
            write_mode=write_mode_internal,
            client=bq_client
        )
        logger.info(f'Data loaded in {bq_project_id}.{bq_dataset}.{table}')

def update(
    meta_client: MetaClient, 
    bq_client: bigquery.Client, 
    bq_project_id: str, 
    bq_dataset: str, 
    ad_account_ids: str | list
):
    utc_minus_3 = timezone(timedelta(hours=-3))
    yesterday = datetime.strftime(datetime.now(utc_minus_3) - timedelta(days=1), format='%Y-%m-%d')
    query = f'''
    SELECT
        MAX(date) AS last_upd
    FROM {bq_project_id}.{bq_dataset}.insights_ads
    '''
    df_insights_upd = bq_client.query(query=query).to_dataframe()
    last_upd = df_insights_upd['last_upd'][0]
    start = datetime.strftime(last_upd + timedelta(days=1), format='%Y-%m-%d')
    
    if last_upd < datetime.strptime(yesterday, '%Y-%m-%d'):
        # Extracting data to dataframes
        df_campaigns, df_adsets, df_ads, df_insights = asyncio.run(
            extract_accounts_async(
                ad_account_ids, 
                meta_client, 
                start
            )
        )
        # Loading tables to BigQuery
        dict_tables = {
            'campaigns': df_campaigns,
            'adsets': df_adsets,
            'ads': df_ads,
            'insights_ads': df_insights
        }
        for table, df in dict_tables.items():
            df_to_bq(
                table_id=f'{bq_project_id}.{bq_dataset}.{table}', 
                df=df, 
                write_mode='truncate' if table != 'insights_ads' else 'append',
                client=bq_client
            )
    elif last_upd == datetime.strptime(yesterday, '%Y-%m-%d'):
        logger.info('Insights already updated until yesterday.')
    else:
        logger.warning('Last update greater that yesterday!!! That should not happen!')