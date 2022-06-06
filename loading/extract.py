import requests
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import numpy as np
import pandas as pd

import time
import datetime as dt

DEFAULT_TIMEOUT = 5 # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

http = requests.Session()

assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
http.hooks["response"] = [assert_status_hook]

retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
http.mount("https://", TimeoutHTTPAdapter(max_retries=retries))


class Discogs:

    def __init__(self, name_discogs_user: str, discogs_token: str, url_discogs_api: str) -> None:
        self.name_discogs_user = name_discogs_user
        self.discogs_token = discogs_token
        self.url_discogs_api = url_discogs_api

    def collection_items(self) -> pd.DataFrame:

        no_pages = 0
        # Get first page to get the number of pages
        try:
            query = {'page': 1, 'per_page': 100}
            url_request = self.url_discogs_api + "/users/" + self.name_discogs_user + "/collection/folders/0/releases"
            response = requests.get(url_request, params=query)
            response.raise_for_status()
            jsonResponse = response.json()
            no_pages = jsonResponse["pagination"]["pages"]
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        if no_pages == 0:
            return 

        # Retrieving all collection items
        collection_items = []
        for i in range(1, no_pages + 1):
            try:
                query = {'page': i, 'per_page': 100}
                url_request = self.url_discogs_api + "/users/" + self.name_discogs_user + "/collection/folders/0/releases"
                response = requests.get(url_request, params=query)
                jsonResponse = response.json()
                collection_items.append(pd.json_normalize(jsonResponse["releases"]))
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')
            except Exception as err:
                print(f'Other error occurred: {err}')

        df_collection = pd.concat(collection_items, ignore_index=True)
    
        return(df_collection)

    def release_lowest_value(self, df_release) -> pd.DataFrame:

        query = {'curr_abbr': 'EUR'}

        lst_lowest_value = []
        for i in df_release.index:
            url_request = self.url_discogs_api + "/marketplace/stats/" + str(df_release['id'][i])
            try:
                response = requests.get(url_request, params=query)
                response.raise_for_status()

                df_item = pd.json_normalize(response.json())
                df_item['id'] = str(df_release['id'][i])
                df_item['df_release'] = dt.datetime.now()
                df_item = df_item.loc[:, df_item.columns != 'lowest_price']
                lst_lowest_value.append(df_item)

            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(60)
            except Exception as err:
                print(f'Other error occurred: {err}')
                
        df_collection_value = pd.concat(lst_lowest_value, ignore_index=True)
        return(df_collection_value)

    def release_stats(self, df_release) -> pd.DataFrame:
        pass
        
    def artists(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        lst_artists = []
        for index, row in df_artists.iterrows():
            try:
                url_request = row['api_artist'] + "?token=" + self.discogs_token
                response = requests.get(url_request)
                lst_artists.append(pd.json_normalize(response.json()))
            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(60)
            except Exception as err:
                print(f'Other error occurred: {err}') 

        df_artist = pd.concat(lst_artists, ignore_index=True)
        return(df_artist)
