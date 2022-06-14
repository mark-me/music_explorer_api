import requests
from requests.auth import AuthBase
from requests.exceptions import HTTPError
from requests.sessions import HTTPAdapter
from requests.adapters import Retry
from requests_toolbelt import sessions
import json
import numpy as np
import pandas as pd

import time
import datetime as dt
import tornado
from tqdm import tqdm

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader


class TokenAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Discogs token'] = f'{self.token}'  # Python 3.6+
        return r        


class Discogs:
    def __init__(self, name_discogs_user: str, discogs_token: str, db_file: str) -> None:
        self.name_discogs_user = name_discogs_user
        self.discogs_token = discogs_token
        self.db_file = db_file
        self.retries = Retry(total=3, backoff_factor=10, status_forcelist=[ 429, 500, 502, 503, 504 ])
        self.session = sessions.BaseUrlSession(base_url='https://api.discogs.com')
        self.session.mount(prefix='https://api.discogs.com', adapter=HTTPAdapter(max_retries=self.retries))

    def __get_qty_pages(self, url_request: str, ) -> int:
        response = self.session.get(
            url_request, 
            params={'page': 1, 'per_page': 100}, 
            auth=TokenAuth(self.discogs_token)
            )
        return response.json()["pagination"]["pages"]

    def collection_items(self) -> None:
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        no_pages = self.__get_qty_pages(
            url_request="/users/" + self.name_discogs_user + "/collection/folders/0/releases"
            )
        if no_pages != 0:
            print("Retrieve collection items")
            for i in tqdm(range(1, no_pages + 1)):
                url_request = "/users/" + self.name_discogs_user + "/collection/folders/0/releases" # Extract 
                try:
                    response = self.session.get(
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
                        self.release(df_release=df_items)
                        time.sleep(2)
                except HTTPError as http_err:
                    if response.status_code == 429:
                        time.sleep(20)
                except Exception as err:
                    print(f'Other error occurred: {err}')

    def release(self, df_release) -> None:
        print("Release community stats")
        db_writer = _db_writer.Collection(db_file=self.db_file)
        query = {'curr_abbr': 'EUR'}
        for index, row in tqdm(df_release.iterrows(), total=df_release.shape[0]):
            url_request = "/releases/" + str(row['id'])
            try:
                response = self.session.get(url_request, params=query, auth=TokenAuth(self.discogs_token))
                response.raise_for_status()
                df_item = pd.json_normalize(response.json())
                df_item['id_release'] = row['id']
                df_item['time_retrieved'] = dt.datetime.now()
                if df_item.shape[0] > 0:
                    derive = _derive.Release(df_release=df_item)
                    db_writer.release_stats(df_release=df_item)
                    db_writer.release_videos(derive.videos())
                    db_writer.release_tracks(derive.tracks())
                time.sleep(2)
            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(20)
            except Exception as err:
                print(f'Other error occurred: {err}')
        
    def artists_collection(self) -> None:
        db_reader = _db_reader.Collection(db_file=self.db_file)
        df_artists = db_reader.new_artists()
        self.artists(df_artists=df_artists)

    def artists_aliases(self) -> None:
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.new_aliases()
        print("Retrieve artist aliases")
        self.artists(df_artists=df_artists)

    def artists_members(self) -> None:
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.new_members()
        print("Retrieve group members")
        self.artists(df_artists=df_artists)

    def artists_groups(self) -> None:
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.new_groups()
        print("Retrieve artist groups")
        self.artists(df_artists=df_artists)

    def artists(self, df_artists: pd.DataFrame) -> None:
        db_writer = _db_writer.Artists(db_file=self.db_file)
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
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
                    time.sleep(2)
            except HTTPError as http_err:
                if response.status_code == 429:
                    time.sleep(20)  
                if response.status_code == 404:
                    df_artist = row.to_frame().T
                    df_artist['not_found'] = 1
                    db_writer.artists(df_artists=df_artist) 
            except Exception as err:
                print(f'Other error occurred: {err}')

    def artist_releases(self, df_artists) -> None:
        collection_reader = _db_reader.Collection(db_file=self.db_file)
        #df_artists = collection_reader.new_artists()
        self.artists(df_artists=df_artists)
 