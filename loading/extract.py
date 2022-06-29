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
        self.__collection_value()
        self.__collection_items()
        self.__artists_from_collection()

    def __collection_value(self) -> None:
        """Process the user's collection value statistics"""
        derive = _derive.Collection(db_file=self.db_file)
        derive.value(self.client.identity())

    def __collection_items(self) -> None:
        """Process the user's collection items"""
        me = self.client.identity()
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        qty_items = me.collection_folders[0].count
        for item in tqdm(me.collection_folders[0].releases, total=qty_items, desc="Collection items"):
            derive = _derive.CollectionItem(item=item, db_file=self.db_file)
            derive.process()

    def __artists_from_collection(self) -> None:
        """Process artist information derived from groups and memberships"""
        db_reader = _db_reader.Collection(db_file=self.db_file)
        qty_artists_not_added = db_reader.qty_artists_not_added()
        while qty_artists_not_added > 0:
            qty_artists_not_added = db_reader.qty_artists_not_added()
            df_artists_new = db_reader.artists_not_added()
            artists = []
            for index, row in df_artists_new.iterrows():
                artists.append(self.client.artist(id=row['id_artist']))
            derive = _derive.Artists(artists=artists, db_file=self.db_file)
            derive.process_masters = False
            derive.process()
            
    def __masters_from_artists(self) -> None:
        """Process master release information from artists"""
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.artists()
        artists = []
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
            artists.append(self.client.artist(id=row['id_artist']))
        derive = _derive.Artists(artists=artists)
        derive.process_masters()

 
class Database(_db_reader._DBStorage):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file)

    def create_edges_view(self) -> None:
        name_view = 'vw_artists_edges'
        self.drop_view(name_view=name_view)
        sql_definition = "SELECT id_artist_from,\
                name_artist_from,\
                id_artist_to,\
                name_artist_to,\
                relation_type,\
                COUNT(*) as qty\
            FROM (\
                SELECT id_member AS id_artist_from,\
                    name_member AS name_artist_from,\
                    artist.id_artist as id_artist_to,\
                    artist.name_artist as name_artist_to,\
                    'group_member' as relation_type\
                FROM artist\
                INNER JOIN artist_members\
                    ON artist_members.id_artist = artist.id_artist\
                UNION\
                    SELECT artist.id_artist as id_artist_from,\
                        artist.name_artist as name_artist_from,\
                        id_group AS id_artist_to,\
                        name_group AS name_artist_to,\
                        'group_member' as relation_type\
                    FROM artist\
                    INNER JOIN artist_groups\
                        ON artist_groups.id_artist = artist.id_artist\
                UNION\
                    SELECT a.id_alias,\
                        a.name_alias,\
                        artist.id_artist,\
                        name_artist,\
                        'artist_alias'\
                    FROM artist\
                    INNER JOIN artist_aliases a\
                        ON a.id_artist = artist.id_artist\
                    LEFT JOIN artist_aliases b\
                        ON a.id_artist = b.id_alias AND\
                            a.id_alias = b.id_artist\
                    WHERE a.id_artist > b.id_artist OR\
                        b.id_artist IS NULL\
                )\
            GROUP BY id_artist_from,\
                name_artist_from,\
                id_artist_to,\
                name_artist_to,\
                relation_type\
            ORDER BY id_artist_from,\
                id_artist_to"
        self.create_view(name_view=name_view, sql_definition=sql_definition)

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
