import requests
from requests.auth import AuthBase
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import numpy as np
import pandas as pd

import time
import datetime as dt

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = 5
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class TokenAuth(AuthBase):
    """Implements a custom authentication scheme."""

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        """Attach an API token to a custom auth header."""
        #r.headers['Authorization: Discogs token'] = f'{self.token}'  # Python 3.6+
        r.headers['Discogs token'] = f'{self.token}'  # Python 3.6+
        return r        
""" 
http = requests.Session()

assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
http.hooks["response"] = [assert_status_hook]

retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
http.mount("https://", TimeoutHTTPAdapter(max_retries=retries)) """


class Discogs:
    def __init__(self, name_discogs_user: str, discogs_token: str, db_file: str) -> None:
        self.name_discogs_user = name_discogs_user
        self.discogs_token = discogs_token
        self.url_discogs_api = 'https://api.discogs.com'
        self.session = requests.Session()
        self.retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 429, 500, 502, 503, 504 ])
        self.db_file = db_file

    def __get_qty_pages(self, url_request: str, ) -> int:
        response = requests.get(
            url_request, 
            params={'page': 1, 'per_page': 100}, 
            auth=TokenAuth(self.discogs_token)
            )
        return response.json()["pagination"]["pages"]

    def collection_items(self) -> None:
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        no_pages = self.__get_qty_pages(
            url_request=self.url_discogs_api + "/users/" + self.name_discogs_user + "/collection/folders/0/releases"
            )
        if no_pages != 0:
            for i in range(1, no_pages + 1):
                url_request = self.url_discogs_api + "/users/" + self.name_discogs_user + "/collection/folders/0/releases" # Extract 
                try:
                    response = requests.get(
                        url_request,
                        params={'page': i, 'per_page': 100}, 
                        auth=TokenAuth(self.discogs_token)
                        )
                    df_items = pd.json_normalize(response.json()["releases"])
                    if df_items.shape[0] > 0:
                        derive = _derive.Collection(df_releases=df_items)   # Derive and store
                        db_writer.items(df_items=df_items)
                        db_writer.artists(df_artists=derive.artists())
                        db_writer.formats(df_formats=derive.formats())
                        db_writer.labels(df_labels=derive.labels())
                        db_writer.genres(df_genres=derive.genres())
                        db_writer.styles(df_styles=derive.styles())
                except HTTPError as http_err:
                    print(f'HTTP error occurred: {http_err}')
                except Exception as err:
                    print(f'Other error occurred: {err}')


    def release_lowest_value(self, df_release) -> None:
        query = {'curr_abbr': 'EUR'}
        lst_lowest_value = []
        for i in df_release.index:
            url_request = self.url_discogs_api + "/marketplace/stats/" + str(df_release['id'][i])
            try:
                response = requests.get(url_request, params=query, auth=TokenAuth(self.discogs_token))
                response.raise_for_status()
                df_item = pd.json_normalize(response.json())
                df_item['id'] = str(df_release['id'][i])
                df_item['df_release'] = dt.datetime.now()
                df_item = df_item.loc[:, df_item.columns != 'lowest_price']
                # Derive stuff
                # Store stuff
                lst_lowest_value.append(df_item)
            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(60)
            except Exception as err:
                print(f'Other error occurred: {err}')

    def release_stats(self, df_release) -> None:
        pass
        
    def artists(self) -> None:
        db_reader = _db_reader.Collection(db_file=self.db_file)
        db_writer = _db_writer.Artists(db_file=self.db_file)
        df_artists = db_reader.new_artists()
        for index, row in df_artists.iterrows():
            try:
                response = self.session.get(row['api_artist'], auth=TokenAuth(self.discogs_token))  # Extract 
                response.raise_for_status()
                df_artist = pd.json_normalize(response.json())
                if df_artist.shape[0] > 0:
                    derive = _derive.Artists(df_artist=df_artist)   # Derive and store
                    db_writer.artists(df_artists=df_artist)
                    db_writer.images(df_images=derive.images())
                    db_writer.urls(df_urls=derive.urls())
                    db_writer.aliases(df_aliases=derive.aliases())
                    db_writer.groups(df_groups=derive.groups())
                    db_writer.members(df_members=derive.members()) 
            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(60)    
            except Exception as err:
                print(f'Other error occurred: {err}')
  
    def artist_releases(self, df_artists) -> None:
        pass
 