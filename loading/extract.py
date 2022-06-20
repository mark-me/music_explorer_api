import os
import time
import datetime as dt
import yaml
import json
import numpy as np
import pandas as pd
import tornado
from tqdm import tqdm
import discogs_client

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader

SLEEP_TOO_MANY_REQUESTS = 5
SLEEP_BETWEEN_CALLS = 2
      

class Discogs:
    def __init__(self, consumer_key: str, consumer_secret: str, db_file: str) -> None:
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.db_file = db_file
        self.client = self.__set_user_tokens()
        
    def __set_user_tokens(self) -> discogs_client.Client:
        has_token = False
        if os.path.isfile('loading/user_tokens.yml'):
            with open(r'loading/user_tokens.yml') as file:
                dict_token = yaml.load(file, Loader=yaml.FullLoader)
            has_token = 'token' in dict_token and 'secret' in dict_token
            
        if not has_token:  
            d = discogs_client.Client(user_agent='music_collection_api/1.0', 
                                      consumer_key=self.consumer_key, 
                                      consumer_secret=self.consumer_secret)   
            url = d.get_authorize_url()
            print("Visit " + url[2] + " to allow the client to access")
            code_verify = input("Enter the verification code: ")
            access_token = d.get_access_token(code_verify)
            dict_token = {'token': access_token[0], 'secret': access_token[1]}
            with open(r'loading/user_tokens.yml', 'w') as file:
                documents = yaml.dump(dict_token, file)
        else:
            d = discogs_client.Client(user_agent='my_user_agent/1.0',
                                      consumer_key=self.consumer_key,
                                      consumer_secret=self.consumer_secret,
                                      token=dict_token['token'],
                                      secret=dict_token['secret'])
        return d

    def start(self) -> None:
        self.collection_value()
        self.collection_items()
        #self.artists_from_collection()

    def collection_value(self) -> None:
        derive = _derive.Collection(db_file=self.db_file)
        derive.value(self.client.identity())

    def collection_items(self) -> None:
        me = self.client.identity()
        qty_items = me.collection_folders[0].count
        for item in tqdm(me.collection_folders[0].releases, total=qty_items):
            derive = _derive.CollectionItem(item=item, db_file=self.db_file)
            derive.start()
            
    def artists_from_collection(self) -> None:
        self.artists_collection()
        db_reader = _db_reader.Artists(db_file=self.db_file)
        qty_artis_not_added = db_reader.get_qty_artis_not_added()
        while qty_artis_not_added > 0:
            self.artists_aliases()
            self.artists_members()
            self.artists_groups()
            qty_artis_not_added = db_reader.get_qty_artis_not_added()

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

    def artist_releases(self, df_artists) -> None:
        collection_reader = _db_reader.Collection(db_file=self.db_file)
        #df_artists = collection_reader.new_artists()
        self.artists(df_artists=df_artists)
 
 
class Database:
    def __init__(self, db_file: str) -> None:
        self.db_file = db_file

    def start(self) -> None:
        db_reader = _db_reader.Artists(db_file=self.db_file)
        db_writer = _db_writer.ArtistNetwork(db_file=self.db_file)
        df_vertices = db_reader.vertices()
        df_edges = db_reader.edges()   
        derive = _derive.ArtistNetwork(df_vertices=df_vertices, df_edges=df_edges)
        if df_edges.shape[0] > 0 and df_vertices.shape[0] > 0:
            db_writer.cluster_hierarchy(df_hierarchy=derive.cluster_betweenness())
            db_writer.centrality(df_centrality=derive.centrality())
            print("Ooohhh.... There is something to cluster")
