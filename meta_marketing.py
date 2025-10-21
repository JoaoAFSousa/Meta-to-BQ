import time
import logging

import requests
import pandas as pd
from pandera.pandas import DataFrameSchema

from table_schemas import *

logger = logging.getLogger(__name__)

def request_w_retries(url, params=None, max_retries=4, base_wait=60):
    """Helper to perform GET with retry logic."""
    retries = 0
    while retries < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response
        else:
            time_to_wait = base_wait * (2 ** retries)
            retries += 1
            if retries < max_retries:
                logger.info(f"Error: {response.text}. Retrying {retries}/{max_retries} in {time_to_wait}s...")
                time.sleep(time_to_wait)
            else:
                raise RuntimeError(f"Failed after {max_retries} retries: {response.text}")
    return None  # safeguard, should never hit

def get_w_pagination(url, params: dict = {}, t_between_calls=1):
    response = request_w_retries(url, params=params)
    if response.status_code == 200:
        response_json =  response.json()
        data = response_json['data']
        while True:
            time.sleep(t_between_calls)
            try:
                next_url = response_json['paging']['next']
            except KeyError:
                # LOOP FOR DEBUGGING
                # logger.info('Last page response:')
                # for key, value in response_json.items():
                #     if key == 'data':
                #         logger.info(f'Data with length: {len(value)}')
                #     else:
                #         logger.info(f'{key} : {value}')
                break
            response = request_w_retries(next_url)
            if response.status_code == 200:
                attempt = 0
                while len(response.json()['data']) == 0 and attempt < 5:  # Handles empty responses
                    logger.warning('Got empty data!!! Retrying API call...')
                    response = request_w_retries(next_url)
                    attempt += 1
                response_json = response.json()
                data.extend(response_json['data'])
            else:
                raise KeyError("Error: ", response.status_code, response.text)  # Response error should be handled in request_w_retries.Probably will not ever fall here.
        return data
    else:
        raise KeyError("Error: ", response.status_code, response.text)
    
def json_to_df_val(data_json: dict, table_schema: DataFrameSchema):
    '''Normalizes json (dict) to dataframe and validates schema.'''
    df = pd.json_normalize(data_json)
    df.columns = [col_name.replace('.', '_') for col_name in df.columns]
    df = table_schema.validate(df) if df.shape != (0, 0) else pd.DataFrame()
    return df

class MetaClient:
    def __init__(self, token: str = None):
        self.token = token
        self.url = 'https://graph.facebook.com/v22.0'
        if not self.token:
            raise ValueError('Please, insert an access token.')        
        url = self.url + '/me'
        params = {
            'fields': 'id,name',
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            self.user_id = response_json['id']
            self.user_name = response_json['name']
        else:
            raise PermissionError(f'API response: {response.text}')
    
    def ad_accounts(self):
        '''Returns ad accounts ID's and names'''
        url = f'{self.url}/{self.user_id}/adaccounts'
        params = {
            'access_token': self.token,
            'limit': 100,
            'fields': 'name,id'
        }
        data = get_w_pagination(url, params=params)
        return data

    def call_insights_data(
            self, 
            level: str, 
            start: str, 
            end: str, 
            ad_account_id: str,
            time_increment: int | str = 1
        ):
        '''
        Calls insights data from ad account, at the level specified.
        level: 'account', 'ad', 'adset' or 'campaign'.
        start/end: date string in the format 'YYYY-MM-DD'.
        '''
        url = f'{self.url}/act_{ad_account_id}/insights'
        fields = [
            # 'account_currency',
            'account_id',
            'account_name',
            'action_values',
            'actions',
            'ad_id',
            'ad_name',
            'adset_id',
            'adset_name',
            # 'attribution_setting',
            'campaign_id',
            'campaign_name',
            # 'clicks',
            # 'conversion_values',
            # 'conversions',
            # 'created_time',
            'date_start',
            'date_stop',
            'frequency',
            'impressions',
            # 'objective',
            # 'optimization_goal',
            'reach',
            'results',
            'spend',
            # 'updated_time'
        ]
        params = {
            'level': level,
            'fields': ','.join(fields),
            'time_range': f"{'{'}'since': '{start}','until': '{end}'{'}'}",
            'time_increment': time_increment,
            'limit': 100,
            'access_token': self.token
        }
        data = get_w_pagination(url, params=params)
        return data

    def df_from_ad_insights(self, start: str, end: str, ad_account_id: str, raw: bool = False):
        '''Takes a .json file from insights data and returns a dataframe'''
        data = self.call_insights_data(level='ad', start=start, end=end, ad_account_id=ad_account_id)
        if raw:
            return data
        if len(data) > 0:
            df = json_to_df_val(data, insights_ads_schema)
            return df
        else:
            return pd.DataFrame()
    
    def df_from_monthly_insights_account(self, start: str, end: str, ad_account_id: str, raw: bool = False):
        data = self.call_insights_data(level='account', start=start, end=end, ad_account_id=ad_account_id, time_increment='monthly')
        if raw:
            return data
        if len(data) > 0:
            df = json_to_df_val(data, insights_account_schema)
            return df
        else:
            return pd.DataFrame()
    
    def df_from_monthly_insights_ads(self, start: str, end: str, ad_account_id: str, raw: bool = False):
        data = self.call_insights_data(level='ad', start=start, end=end, ad_account_id=ad_account_id, time_increment='monthly')
        if raw:
            return data
        if len(data) > 0:
            df = json_to_df_val(data, insights_ads_schema)
            return df
        else:
            return pd.DataFrame()
    
    def df_from_monthly_insights_campaigns(self, start: str, end: str, ad_account_id: str, raw: bool = False):
        data = self.call_insights_data(level='campaign', start=start, end=end, ad_account_id=ad_account_id, time_increment='monthly')
        if raw:
            return data
        if len(data) > 0:
            df = json_to_df_val(data, insights_campaign_schema)
            return df
        else:
            return pd.DataFrame()
    
    def df_from_ads(self, ad_account_id: str, raw: bool = False):
        '''Calls data from ads and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/ads'
        fields = [
            'id',
            'account_id',
            # 'ad_active_time',
            # 'ad_review_feedback',
            'ad_schedule_end_time',
            'ad_schedule_start_time',
            # 'adlabels',
            'adset_id',
            # 'bid_amount',
            'campaign_id',
            'configured_status',
            # 'conversion_domain',
            # 'created_time',
            'creative',
            # 'creative_asset_groups_spec',
            'effective_status',
            # 'issues_info',
            # 'last_updated_by_app_id',
            'name',
            'preview_shareable_link',
            # 'recommendations',
            'source_ad_id',
            'status',
            # 'tracking_specs',
            # 'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        ads_data = get_w_pagination(url, params=params)
        if not raw:
            df = json_to_df_val(ads_data, table_schema=ads_schema)
            return df
        else:
            return ads_data

    def df_from_adsets(self, ad_account_id: str, raw: bool = False):
        '''Calls data from adsets and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/adsets'
        fields = [
            'id',
            'account_id',
            # 'adlabels',
            # 'adset_schedule',
            # 'attribution_spec',
            # 'budget_remaining',
            # 'campaign_active_time',
            # 'campaign_attribution',
            'campaign_id',
            'configured_status',
            # 'created_time',
            'daily_budget',
            # 'daily_min_spend_target',
            # 'daily_spend_cap',
            # 'destination_type',
            'effective_status',
            'end_time',
            'name',
            'optimization_goal',
            # 'optimization_sub_event',
            # 'pacing_type',
            'promoted_object',
            'source_adset_id',
            # 'start_time',
            'status',
            # 'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        adsets = get_w_pagination(url, params=params)
        if not raw:
            df = json_to_df_val(adsets, adsets_schema)
            return df
        else:
            return adsets

    def df_from_campaigns(self, ad_account_id: str, raw: bool = False):
        '''Calls data from campaigns and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/campaigns'
        fields = [
            'id',
            'account_id',
            # 'adlabels',
            # 'bid_strategy',
            'boosted_object_id',
            # 'budget_rebalance_flag',
            # 'budget_remaining',
            # 'buying_type',
            # 'can_use_spend_cap',
            'configured_status',
            # 'created_time',
            'daily_budget',
            'effective_status',
            # 'is_adset_budget_sharing_enabled',
            # 'is_budget_schedule_enabled',
            # 'is_direct_send_campaign',
            # 'is_message_campaign',
            # 'issues_info',
            # 'last_budget_toggling_time',
            'lifetime_budget',
            'name',
            'objective',
            # 'pacing_type',
            # 'primary_attribution',
            # 'promoted_object',
            # 'smart_promotion_type',
            'source_campaign_id',
            # 'spend_cap',
            'start_time',
            'status',
            'stop_time',
            # 'topline_id',
            # 'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        campaigns = get_w_pagination(url, params=params)
        if not raw:
            df = json_to_df_val(campaigns, campaigns_schema)
            return df
        else:
            return campaigns
         
    def df_from_adcreatives(self, ad_account_id: str, raw: bool = False):
        '''Calls data from adcreatives and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/adcreatives'
        fields = [
            'id',
            'account_id',
            # 'body',
            # 'effective_instagram_media_id',
            # 'effective_object_story_id',
            'instagram_permalink_url',
            # 'name',
            'status',
            # 'thumbnail_url',
            'title'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        adcreatives = get_w_pagination(url, params=params)
        if not raw:
            df_adcreatives = json_to_df_val(adcreatives, adcreatives_schema)
            return df_adcreatives
        else:
            return adcreatives
