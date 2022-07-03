import sqlite3
from unicodedata import name
import numpy as np
import pandas as pd


class _DBStorage():
    """A base class for storing discogs data
    """
    def __init__(self, db_file) -> None:
        self.db_file = db_file  

    def write_data(self, df: pd.DataFrame, name_table: str) -> None:
        """Write data to the database"""
        if not df.empty: 
            self.create_table(name_table=name_table)
            self.store_append(df=df, name_table=name_table)
            
    def create_table(self, name_table: str) -> None:
        """Virtual function for creating tables"""
        pass
               
    def table_exists(self, name_table: str) -> bool:
        """Checks whether a table exists"""
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + name_table + "'")
        does_exist = cursor.fetchone()[0]==1 
        db_con.close()
        return does_exist

    def view_exists(self, name_view: str) -> bool:
        """Checks whether a view exists"""
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='view' AND name='" + name_view + "'")
        does_exist = cursor.fetchone()[0]==1 
        db_con.close()
        return does_exist

    def drop_existing_table(self, name_table: str) -> None:
        """Dropping a table"""
        if self.table_exists(name_table): 
            db_con = sqlite3.connect(self.db_file)
            cursor = db_con.cursor()
            cursor.execute("DROP TABLE " + name_table)
            db_con.commit()
            db_con.close()

    def store_replace(self, df: pd.DataFrame, name_table: str) -> None:
        """Storing data that """
        db_con = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db_con, if_exists='replace', index=False) 
        db_con.close() 

    def store_append(self, df: pd.DataFrame, name_table: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db_con, if_exists='append', index=False) 
        db_con.close()   


class Collection(_DBStorage):
    """A class for storing collection item data
    """
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def drop_tables(self) -> None:
        self.drop_existing_table(name_table='collection_items')

    def create_views(self) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql_file = open("loading/create_views.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

    def value(self, df_value: pd.DataFrame) -> None:
        self.store_append(df=df_value, name_table='collection_value')

    def items(self, df_items: pd.DataFrame) -> None:
        if df_items.shape[0] > 0:
            self.store_append(df=df_items, name_table='collection_items')

    def artists(self, df_artists: pd.DataFrame) -> None:
        self.store_append(df=df_artists, name_table='collection_artists')

    def formats(self, df_formats: pd.DataFrame) -> None:
        if df_formats.shape[0] > 0: 
            self.store_append(df=df_formats, name_table='collection_formats')

    def labels(self, df_labels: pd.DataFrame) -> None:
        if df_labels.shape[0] > 0: 
            self.store_append(df=df_labels, name_table='collection_labels')

    def genres(self, df_genres: pd.DataFrame) -> None:
        if df_genres.shape[0] > 0: 
            self.store_append(df=df_genres, name_table='collection_genres')  

    def styles(self, df_styles: pd.DataFrame) -> None:
        if df_styles.shape[0] > 0:
            self.store_append(df=df_styles, name_table='collection_styles')    


class Artists(_DBStorage):
    """A class for storing artist related data
    """
    def __init__(self, db_file) -> None:
        super().__init__(db_file)
        
    def in_db(self, id_artist: int) -> bool:
        """Checks whether the artist is already in the database"""
        if self.table_exists(name_table='artist'):
            db_con = sqlite3.connect(self.db_file)
            cursor = db_con.cursor()
            cursor.execute("SELECT count(*) FROM artist WHERE id_artist=" + str(id_artist) + "")
            does_exist = cursor.fetchone()[0] > 0 
        else:
            does_exist = False
        return does_exist

    def create_table(self, name_table: str) -> None:
        """Creates artist related tables in the database if they don't exist"""
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        if not self.table_exists(name_table=name_table):
            sql = ""
            if name_table == 'artist':
                sql = "CREATE TABLE artist(name_artist TEXT, id_artist INT, role TEXT);"
            elif name_table == 'artist_masters':
                sql = "CREATE TABLE artist_masters (id_artist INTEGER, id_master INTEGER, title TEXT, type TEXT, id_main_release INTEGER,\
                    name_artist TEXT, role TEXT, year INTEGER, url_thumb TEXT)"
            elif name_table == 'artist_aliases':
                sql = "CREATE TABLE artist_aliases (id_alias INTEGER, name_alias TEXT, api_alias TEXT, id_artist INTEGER,\
                    url_thumbnail TEXT);"
            elif name_table == 'artist_members':
                sql = "CREATE TABLE artist_members (id_member INTEGER, name_member TEXT, api_member TEXT, is_active INTEGER, id_artist INTEGER,\
                    url_thumbnail TEXT)"
            elif name_table == 'artist_groups':
                sql = "CREATE TABLE artist_groups (id_group INTEGER, name_group TEXT, api_group TEXT, is_active INTEGER, id_artist INTEGER,\
                    url_thumbnail TEXT)"
            elif name_table == 'artist_images':
                sql = "CREATE TABLE artist_images (id_artist INTEGER, type TEXT,\
                    url_image TEXT, url_image_150 TEXT, width_image INTEGER, height_image INTEGER)"
            elif name_table == 'artist_urls':
                sql = "CREATE TABLE artist_urls (url_artist TEXT, id_artist INTEGER)"
            cursor.execute(sql)

    def artists(self, df_artists: pd.DataFrame) -> None:
        """Store the artist"""
        self.write_data(df=df_artists, name_table='artist')

    def masters(self, df_masters: pd.DataFrame) -> None:
        """Store artist master releases"""
        self.write_data(df=df_masters, name_table='artist_masters')

    def aliases(self, df_aliases: pd.DataFrame) -> None:
        """Store the artist's alias(es)"""
        self.write_data(df=df_aliases, name_table='artist_aliases')

    def members(self, df_members: pd.DataFrame) -> None:
        """Store the group's members"""
        self.write_data(df=df_members, name_table='artist_members')

    def groups(self, df_groups: pd.DataFrame) -> None:
        """Store the artist's groups she/he was part of"""
        self.write_data(df=df_groups, name_table='artist_groups')

    def images(self, df_images: pd.DataFrame) -> None:
        """Store the artist's image(s)"""
        self.write_data(df=df_images, name_table='artist_images')

    def urls(self, df_urls: pd.DataFrame) -> None:
        """Store the artist's url(s)"""
        self.write_data(df=df_urls, name_table='artist_urls')


class Master(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def in_db(self, id_master: int) -> bool:
        """Checks if the master is already in the database"""
        if self.table_exists(name_table='master'):
            db_con = sqlite3.connect(self.db_file)
            cursor = db_con.cursor()
            cursor.execute("SELECT count(*) FROM master WHERE id_master=" + str(id_master) + "")
            does_exist = cursor.fetchone()[0] > 0 
        else:
            does_exist = False
        return does_exist    
 
    def master(self, df_master: pd.DataFrame) -> None:
        """Store the master"""
        if df_master.shape[0] > 0:
            self.store_append(df=df_master, name_table='master')

    def genres(self, df_genres: pd.DataFrame) -> None:
        """Store the master's genre(s)"""
        if df_genres.shape[0] > 0: 
            self.store_append(df=df_genres, name_table='master_genres')  

    def styles(self, df_styles: pd.DataFrame) -> None:
        """Store the master's style(s)"""
        if df_styles.shape[0] > 0:
            self.store_append(df=df_styles, name_table='master_styles')    

    def videos(self, df_videos: pd.DataFrame) -> None:
        """Store the master's video(s)"""
        if df_videos.shape[0] > 0:
            self.store_append(df=df_videos, name_table='master_videos') 

    def tracks(self, df_tracks: pd.DataFrame) -> None:
        """Store the master's track(s)"""
        if df_tracks.shape[0] > 0:
            self.store_append(df=df_tracks, name_table='master_tracks') 
        
    def track_artist(self, df_artists: pd.DataFrame) -> None:
        """Store the master's track artist(s)"""
        if df_artists.shape[0] > 0: 
            self.store_append(df=df_artists, name_table='master_track_artists')    

    def stats(self, df_stats: pd.DataFrame) -> None:
        """Store the master's marketplace statistics"""
        if df_stats.shape[0] > 0:
            self.store_append(df=df_stats, name_table='master_stats')


class Release(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def in_db(self, id_release: int) -> bool:
        """Checks if the release is already in the database"""
        if self.table_exists(name_table='release'):
            db_con = sqlite3.connect(self.db_file)
            cursor = db_con.cursor()
            cursor.execute("SELECT count(*) FROM release WHERE id_release=" + str(id_release) + "")
            does_exist = cursor.fetchone()[0] > 0 
        else:
            does_exist = False
        return does_exist    

    def release(self, df_release: pd.DataFrame) -> None:
        """Store the release"""
        if df_release.shape[0] > 0:
            self.store_append(df=df_release, name_table='release')

    def artists(self, df_artists: pd.DataFrame) -> None:
        """Store the release's artist"""
        if df_artists.shape[0] > 0:
            self.store_append(df=df_artists, name_table='release_artists')

    def credits(self, df_credits: pd.DataFrame) -> None:
        """Store the release's artist credits"""
        if df_credits.shape[0] > 0:
            self.store_append(df=df_credits, name_table='release_credits')

    def formats(self, df_formats: pd.DataFrame) -> None:
        """Store the release's format(s)"""
        if df_formats.shape[0] > 0: 
            self.store_append(df=df_formats, name_table='release_formats')

    def labels(self, df_labels: pd.DataFrame) -> None:
        """Store the release's label(s)"""
        if df_labels.shape[0] > 0: 
            self.store_append(df=df_labels, name_table='release_labels')

    def genres(self, df_genres: pd.DataFrame) -> None:
        """Store the release's genre(s)"""
        if df_genres.shape[0] > 0: 
            self.store_append(df=df_genres, name_table='release_genres')  

    def styles(self, df_styles: pd.DataFrame) -> None:
        """Store the release's style(s)"""
        if df_styles.shape[0] > 0:
            self.store_append(df=df_styles, name_table='release_styles')    

    def videos(self, df_videos: pd.DataFrame) -> None:
        """Store the release's video(s)"""
        if df_videos.shape[0] > 0:
            self.store_append(df=df_videos, name_table='release_videos') 

    def tracks(self, df_tracks: pd.DataFrame) -> None:
        """Store the release's track(s)"""
        if df_tracks.shape[0] > 0:
            self.store_append(df=df_tracks, name_table='release_tracks') 

    def track_artist(self, df_artists: pd.DataFrame) -> None:
        """Store the release's track artist(s)"""
        if df_artists.shape[0] > 0: 
            self.store_append(df=df_artists, name_table='release_track_artists')    

    def stats(self, df_stats: pd.DataFrame) -> None:
        """Store the release's marketplace statistics"""
        if df_stats.shape[0] > 0:
            self.store_append(df=df_stats, name_table='release_stats')


class ArtistNetwork(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def vertices(self, df_vertices: pd.DataFrame) -> None:
        pass

    def edges(self, df_edges: pd.DataFrame) -> None:
        pass

    def cluster_hierarchy(self, df_hierarchy: pd.DataFrame) -> None:
        if df_hierarchy.shape[0] > 0:
            self.drop_existing_table(name_table='artist_community_hierarchy')
            self.store_append(df=df_hierarchy, name_table='artist_community_hierarchy')
        pass

    def centrality(self, df_centrality: pd.DataFrame) -> None:
        pass

    def cluster_navigation(self, df_navigation: pd.DataFrame) -> None:
        pass
