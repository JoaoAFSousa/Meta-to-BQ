import time

import requests
import pandas as pd

from table_schemas import *

def request_w_retries(url, params=None, max_retries=3, wait_seconds=20):
    """Helper to perform GET with retry logic."""
    retries = 0
    while retries < max_retries:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response
        else:
            retries += 1
            if retries < max_retries:
                print(f"Error: {response.text}. Retrying {retries}/{max_retries} in {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                raise KeyError(f"Failed after {max_retries} retries: {response.text}")
    return None  # safeguard, should never hit

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
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            data = response_json['data']
            while True:
                try:
                    url = response_json['paging']['next']
                    response = requests.get(url)
                    if response.status_code == 200:
                        response_json = response.json()
                        data.extend(response_json['data'])
                    else:
                        raise ValueError(response.text)
                except KeyError:
                    break
            return data
        else:
            raise ValueError(response.text)

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
            'account_currency',
            'account_id',
            'account_name',
            'action_values',
            'actions',
            'ad_id',
            'ad_name',
            'adset_id',
            'adset_name',
            'attribution_setting',
            'campaign_id',
            'campaign_name',
            'clicks',
            'conversion_values',
            'conversions',
            'created_time',
            'date_start',
            'date_stop',
            'frequency',
            'impressions',
            'objective',
            'optimization_goal',
            'reach',
            'results',
            'spend',
            'updated_time'
        ]
        params = {
            'level': level,
            'fields': ','.join(fields),
            'time_range': f"{'{'}'since': '{start}','until': '{end}'{'}'}",
            'time_increment': time_increment,
            'limit': 100,
            'access_token': self.token
        }
        response = request_w_retries(url, params=params) # requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            data = response_json['data']
            while True: 
                try:
                    time.sleep(1) # Add parameter
                    url = response_json['paging']['next']
                    response = request_w_retries(url) # requests.get(url=url)
                    if response.status_code == 200:
                        response_json = response.json()
                        data.extend(response_json['data'])
                    else:
                        raise KeyError(response.text)
                except KeyError:
                    break
            return data
        else:
            raise KeyError(response.text)

    def df_from_ad_insights(self, start: str, end: str, ad_account_id: str, raw: bool = False):
        '''Takes a .json file from insights data and returns a dataframe'''
        data = self.call_insights_data(level='ad', start=start, end=end, ad_account_id=ad_account_id)
        if raw:
            return data
        if len(data) > 0:
            df = pd.json_normalize(data)
            df.columns = [col_name.replace('.', '_') for col_name in df.columns]
            df = insights_ads_schema.validate(df)
            return df
        else:
            return pd.DataFrame()
    
    def df_from_ads(self, ad_account_id: str, raw: bool = False):
        '''Calls data from ads and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/ads'
        fields = [
            'id',
            'account_id',
            'ad_active_time',
            'ad_review_feedback',
            'ad_schedule_end_time',
            'ad_schedule_start_time',
            'adlabels',
            'adset_id',
            'bid_amount',
            'campaign_id',
            'configured_status',
            'conversion_domain',
            'created_time',
            'creative',
            'creative_asset_groups_spec',
            'effective_status',
            'issues_info',
            'last_updated_by_app_id',
            'name',
            'preview_shareable_link',
            'recommendations',
            'source_ad_id',
            'status',
            'tracking_specs',
            'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            ads_data = response_json['data']
            while True:
                try:
                    time.sleep(1) # Add parameter
                    response = requests.get(response_json['paging']['next'])
                    if response.status_code == 200:
                        response_json = response.json()
                        ads_data.extend(response_json['data'])
                    else:
                        raise KeyError(response.text)
                except KeyError:
                    break
            if not raw:
                df = pd.json_normalize(ads_data)
                df.columns = [col_name.replace('.', '_') for col_name in df.columns]
                df = ads_schema.validate(df) if df.shape != (0, 0) else pd.DataFrame()
                return df
            else:
                return ads_data
        else:
            raise KeyError(response.text)

    def df_from_adsets(self, ad_account_id: str, raw: bool = False):
        '''Calls data from adsets and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/adsets'
        fields = [
            'id',
            'account_id',
            'adlabels',
            'adset_schedule',
            'attribution_spec',
            'budget_remaining',
            'campaign_active_time',
            'campaign_attribution',
            'campaign_id',
            'configured_status',
            'created_time',
            'daily_budget',
            'daily_min_spend_target',
            'daily_spend_cap',
            'destination_type',
            'effective_status',
            'end_time',
            'name',
            'optimization_goal',
            'optimization_sub_event',
            'pacing_type',
            'promoted_object',
            'source_adset_id',
            'start_time',
            'status',
            'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            adsets = response_json['data']
            while True:
                try:
                    time.sleep(1) # Add parameter
                    response = requests.get(response_json['paging']['next'])
                    if response.status_code == 200:
                        response_json = response.json()
                        adsets.extend(response_json['data'])
                    else:
                        raise KeyError(response.text)
                except KeyError:
                    break
            if not raw:
                df = pd.json_normalize(adsets)
                df.columns = [col_name.replace('.', '_') for col_name in df.columns]
                df = adsets_schema.validate(df) if df.shape != (0, 0) else pd.DataFrame()
                return df
            else:
                return adsets
        else:
            raise KeyError(response.text)

    def df_from_campaigns(self, ad_account_id: str, raw: bool = False):
        '''Calls data from campaigns and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/campaigns'
        fields = [
            'id',
            'account_id',
            'adlabels',
            'bid_strategy',
            'boosted_object_id',
            'budget_rebalance_flag',
            'budget_remaining',
            'buying_type',
            'can_use_spend_cap',
            'configured_status',
            'created_time',
            'daily_budget',
            'effective_status',
            'is_adset_budget_sharing_enabled',
            'is_budget_schedule_enabled',
            'is_direct_send_campaign',
            'is_message_campaign',
            'issues_info',
            'last_budget_toggling_time',
            'lifetime_budget',
            'name',
            'objective',
            'pacing_type',
            'primary_attribution',
            'promoted_object',
            'smart_promotion_type',
            'source_campaign_id',
            'spend_cap',
            'start_time',
            'status',
            'stop_time',
            'topline_id',
            'updated_time'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            campaigns = response_json['data']
            while True:
                try:
                    time.sleep(1) # Add parameter
                    response = requests.get(response_json['paging']['next'])
                    if response.status_code == 200:
                        response_json = response.json()
                        campaigns.extend(response_json['data'])
                    else:
                        raise KeyError(response.text)
                except KeyError:
                    break
            if not raw:
                df = pd.json_normalize(campaigns)
                df.columns = [col_name.replace('.', '_') for col_name in df.columns]
                df = campaigns_schema.validate(df) if df.shape != (0, 0) else pd.DataFrame()
                return df
            else:
                return campaigns
        else:
            raise KeyError(response.text)