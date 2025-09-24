import requests
import pandas as pd

from table_schemas import *

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

    def call_insights_data(self, level: str, start: str, end: str, ad_account_id: str):
        '''
        Calls insights data from ad account, at the level specified.
        level: 'account', 'ad', 'adset' or 'campaign'.
        start/end: date string in the format 'YYYY-MM-DD'.
        '''
        url = f'{self.url}/act_{ad_account_id}/insights'
        fields = [
            'account_id',
            'account_name',
            f'{level}_id',
            f'{level}_name',
            'objective',
            'optimization_goal',
            'impressions',
            'reach',
            'actions',
            'spend',
            'video_p25_watched_actions',
            'video_p50_watched_actions',
            'video_p75_watched_actions',
            'video_p95_watched_actions',
            'video_p100_watched_actions'
        ]
        params = {
            'level': level,
            'fields': ','.join(fields),
            'time_range': f"{'{'}'since': '{start}','until': '{end}'{'}'}",
            'time_increment': 1,
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            data = response_json['data']
            while True: 
                try:
                    url = response_json['paging']['next']
                    response = requests.get(url=url)
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

    def df_from_ad_insights(self, start: str, end: str, ad_account_id: str):
        '''Takes a .json file from insights data and returns a dataframe'''
        data = self.call_insights_data(level='ad', start=start, end=end, ad_account_id=ad_account_id)
        normal_data = []
        if len(data) > 0:
            for item in data:
                try: 
                    action_fields = {f"action_{action['action_type']}": action['value'] for action in item['actions']}
                except KeyError:
                    action_fields = {}
                normal_item = {
                    'date': item['date_start'],
                    'account_id': item['account_id'],
                    'account_name': item['account_name'],
                    'ad_id': item['ad_id'],
                    'ad_name': item['ad_name'],
                    'objective': item['objective'],
                    'optimization_goal': item['optimization_goal'],
                    'impressions': item.get('impressions', 0),
                    'reach': item.get('reach', 0),
                    'video_p25_watched_actions': item.get('video_p25_watched_actions', [{}])[0].get('value', None),
                    'video_p50_watched_actions': item.get('video_p50_watched_actions', [{}])[0].get('value', None),
                    'video_p75_watched_actions': item.get('video_p75_watched_actions', [{}])[0].get('value', None),
                    'video_p95_watched_actions': item.get('video_p95_watched_actions', [{}])[0].get('value', None),
                    'video_p100_watched_actions': item.get('video_p100_watched_actions', [{}])[0].get('value', None),
                    'spend': item['spend']
                }
                normal_item.update(action_fields)
                normal_data.append(normal_item)
            df = pd.json_normalize(normal_data)
            df.columns = [col_name.replace('.', '_') for col_name in df.columns]
            df['date'] = pd.to_datetime(df['date'])
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