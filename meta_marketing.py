# Importing libraries
import requests
import pandas as pd

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
            'fields': 'name'
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            data = response_json['data']
            while True:
                try:
                    url = response_json['paging']['next']
                    response = requests.get(url)
                    response_json = response.json()
                    data.extend(response_json['data'])
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
            'spend'
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
                    response_json = response.json()
                    data.extend(response_json['data'])
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
                    'impressions': item['impressions'],
                    'reach': item['reach'],
                    'spend': item['spend']
                }
                normal_item.update(action_fields)
                normal_data.append(normal_item)
            df = pd.json_normalize(normal_data)
            df.columns = [col_name.replace('.', '_') for col_name in df.columns]
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            return pd.DataFrame()
    
    def df_from_ads(self, ad_account_id: str):
        '''Calls data from ads and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/ads'
        fields = [
            'account_id',
            'account_name',
            'created_time',
            'id',
            'adset_id',
            'campaign_id',
            'status',
            'name',
            'ad_active_time',
            'creative',
            'source_ad_id',
            'preview_shareable_link'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        response_json = response.json()
        ads_data = response_json['data']
        while True:
            try:
                response = requests.get(response_json['paging']['next'])
                response_json = response.json()
                ads_data.extend(response_json['data'])
            except KeyError:
                break

        df = pd.json_normalize(ads_data)
        df.columns = [col_name.replace('.', '_') for col_name in df.columns]
        return df

    def df_from_adsets(self, ad_account_id: str):
        '''Calls data from adsets and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/adsets'
        fields = [
            'account_id',
            'account_name',
            'created_time',
            'end_time',
            'id',
            'name',
            'status',
            'campaign_id',
            'billing_event',
            'daily_budget',
            'destination_type',
            'optimization_goal',
            'promoted_object',
            'source_adset_id'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        response_json = response.json()
        adsets = response_json['data']
        while True:
            try:
                response = requests.get(response_json['paging']['next'])
                response_json = response.json()
                adsets.extend(response_json['data'])
            except KeyError:
                break

        df = pd.json_normalize(adsets)
        df.columns = [col_name.replace('.', '_') for col_name in df.columns]
        return df

    def df_from_campaigns(self, ad_account_id: str):
        '''Calls data from campaigns and returns it in a dataframe'''
        url = f'{self.url}/act_{ad_account_id}/campaigns'
        fields = [
            'account_id',
            'account_name',
            'id',
            'name',
            'status',
            'created_time',
            'updated_time',
            'stop_time',
            'daily_budget',
            'objective',
            'source_campaign_id',
            'boosted_object_id'
        ]
        params = {
            'fields': ','.join(fields),
            'date_preset': 'maximum',
            'limit': 100,
            'access_token': self.token
        }
        response = requests.get(url, params=params)
        response_json = response.json()
        campaigns = response_json['data']
        while True:
            try:
                response = requests.get(response_json['paging']['next'])
                response_json = response.json()
                campaigns.extend(response_json['data'])
            except KeyError:
                break

        df = pd.json_normalize(campaigns)
        df.columns = [col_name.replace('.', '_') for col_name in df.columns]
        return df