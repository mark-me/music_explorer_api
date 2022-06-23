import os
import datetime as dt
import yaml
import numpy as np
import pandas as pd
from tqdm import tqdm
import discogs_client

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader


class Discogs:
    """A class for extracting, processing and storing a user's collection data from Discogs
    """
    def __init__(self, consumer_key: str, consumer_secret: str, db_file: str) -> None:
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.db_file = db_file
        self.client = self.__set_user_tokens()
        
    def __set_user_tokens(self) -> discogs_client.Client:
        """Set-up the user's account to use with for the extraction using Discogs API"""
        file_user_token = '/data/user_tokens.yml'
        has_token = False
        if os.path.isfile(file_user_token):
            with open(file_user_token) as file:
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
            with open(file_user_token, 'w') as file:
                documents = yaml.dump(dict_token, file)
        else:
            d = discogs_client.Client(user_agent='music_collection_api/1.0',
                                      consumer_key=self.consumer_key,
                                      consumer_secret=self.consumer_secret,
                                      token=dict_token['token'],
                                      secret=dict_token['secret'])
        return d

    def start(self) -> None:
        """Starts user's collection processing"""
        #self.collection_value()
        #self.collection_items()
        self.artists_from_collection()

    def collection_value(self) -> None:
        """Process the user's collection value statistics"""
        derive = _derive.Collection(db_file=self.db_file)
        derive.value(self.client.identity())

    def collection_items(self) -> None:
        """Process the user's collection items"""
        me = self.client.identity()
        qty_items = me.collection_folders[0].count
        for item in tqdm(me.collection_folders[0].releases, total=qty_items):
            derive = _derive.CollectionItem(item=item, db_file=self.db_file)
            derive.process()
            
    def artists_from_collection(self) -> None:
        """Process artist information derived from groups and memberships"""
        db_reader = _db_reader.Collection(db_file=self.db_file)
        qty_artis_not_added = db_reader.qty_artists_not_added()
        while qty_artis_not_added > 0:
            qty_artis_not_added = db_reader.qty_artists_not_added()
            df_artists_new = db_reader.artists_not_added()
            lst_artists = []
            for index, row in df_artists_new.iterrows():
                lst_artists.append(self.client.artist(id=row['id_artist']))
            derive = _derive.Artists(artist=lst_artists, db_file=self.db_file)
            derive.process()
            
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
