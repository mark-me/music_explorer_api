import os
import datetime as dt
import yaml
import sqlite3
import numpy as np
import pandas as pd
import igraph as ig
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
        #self.__collection_value()
        #self.__collection_items()
        #self.__artist_is_group()
        #self.__artist_thumbnail()
        self.__artist_collection_items()
        self.__extract_artist_edges()
        self.__extract_artist_to_ignore()
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

    def masters_from_artists(self) -> None:
        """Process master release information from artists"""
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.artists()
        artists = []
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
            artists.append(self.client.artist(id=row['id_artist']))
        derive = _derive.Artists(artists=artists)
        derive.process_masters()

    def __artist_is_group(self) -> None:
        sql = "UPDATE artist SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist)"
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute(sql)
        db_con.close()
                
    def __artist_thumbnail(self) -> None:
        sql = "UPDATE artist\
                SET url_thumbnail = (\
                    SELECT url_thumbnail FROM (\
                        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases\
                        UNION\
                    SELECT id_member, url_thumbnail FROM artist_members\
                        UNION\
                        SELECT id_group, url_thumbnail FROM artist_groups\
                    ) WHERE id_artist = artist.id_artist )"
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute(sql)
        db_con.close()
                    
    def __artist_collection_items(self) -> None:
        sql = "UPDATE artist\
            SET qty_collection_items = (SELECT COUNT(*)\
                        FROM collection_items\
                        INNER JOIN release_artists\
                            ON release_artists.id_release = collection_items.id_release \
                        WHERE  release_artists.id_artist = artist.id_artist)"
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute(sql)
        db_con.close()

    def __extract_artist_edges(self) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql_file = open("loading/sql/extract_artist_relations.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)
        
    def __artist_vertices(self) -> pd.DataFrame:
        sql = "SELECT id_artist, MAX(in_collection) AS in_collection\
            FROM (\
                SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist\
                UNION\
                SELECT id_alias, 0 as in_collection from artist_aliases\
                UNION\
                SELECT id_member, 0 FROM artist_members\
                UNION\
                SELECT id_group, 0 FROM artist_groups\
                UNION\
                SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')\
            )\
            GROUP BY id_artist"
        db_con = sqlite3.connect(self.db_file)
        df_vertices = pd.read_sql_query(sql=sql, con=db_con)
        db_con.close() 
        return df_vertices
        
    def __extract_artist_to_ignore(self) -> None:
        artists = _db_reader.Artists(db_file=self.db_file)
        graph = ig.Graph.DataFrame(edges=artists.edges(), directed=False, 
                                    vertices=self.__artist_vertices())
        # Select relevant vertices
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_relevant = []
        for vtx in vtx_collection:
            vtx_neighbors = graph.neighborhood(vertices=vtx, order=2)           # Only query those that have less than 3 steps
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_relevant = list(set(vtx_neighbors + vtx_relevant))
            vtx_connectors = graph.get_shortest_paths(vtx, to=vtx_collection)   # Vertices that connect artists in the collection
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_relevant = list(set(vtx_connectors + vtx_relevant))
        # Get vertices to ignore
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_relevant))
        df_ignore = pd.DataFrame({'id_artist': graph.vs[vtx_to_exclude]['name']})
        db_writer = _db_writer.Artists(db_file=self.db_file)
        db_writer.ignore_list(df_ignore=df_ignore)