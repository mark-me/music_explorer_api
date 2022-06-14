import sqlite3
from unicodedata import name
import numpy as np
import pandas as pd

class _DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file  

    def table_exists(self, name_table: str) -> bool:
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='" + name_table + "'")
        does_exist = cursor.fetchone()[0]==1 
        return does_exist
    
    def view_exists(self, name_view: str) -> bool:
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='view' AND name='" + name_view + "'")
        does_exist = cursor.fetchone()[0]==1 
        return does_exist

    def drop_existing_table(self, name_table: str) -> None:
        if self.table_exists(name_table): 
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("DROP TABLE " + name_table)
            conn.commit()
            conn.close()

    def store_replace(self, df: pd.DataFrame, name_table: str) -> None:
        db = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db, if_exists='replace', index=False) 
        db.close() 

    def store_append(self, df: pd.DataFrame, name_table: str) -> None:
        db = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db, if_exists='append', index=False) 
        db.close()   

class Collection(_DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def drop_tables(self) -> None:
        self.drop_existing_table(name_table='collection_items')
        self.drop_existing_table(name_table='collection_artists')
        self.drop_existing_table(name_table='collection_formats')
        self.drop_existing_table(name_table='collection_labels')
        self.drop_existing_table(name_table='collection_genres')
        self.drop_existing_table(name_table='collection_styles')
        self.drop_existing_table(name_table='collection_videos')
        self.drop_existing_table(name_table='collection_tracks')
        
    def items(self, df_items: pd.DataFrame) -> None:
        if df_items.shape[0] == 0: return
        selected_columns = df_items.columns[~df_items.columns.isin([ 'basic_information.thumb', 'basic_information.cover_image',\
            'basic_information.artists', 'basic_information.labels', 'basic_information.formats', 'basic_information.genres','basic_information.styles' ])]
        df_items = df_items[selected_columns]
        df_items = df_items.rename(columns={'id': 'id_release', 'instance_id': 'id_instance',\
            'basic_information.id': 'id_basic_info', 'basic_information.master_id': 'id_master', 'basic_information.master_url': 'url_master',\
            'basic_information.resource_url' : 'url_release'})
        self.store_append(df=df_items, name_table='collection_items')

    def artists(self, df_artists: pd.DataFrame) -> None:
        self.store_append(df=df_artists, name_table='collection_artists')

    def formats(self, df_formats: pd.DataFrame) -> None:
        if df_formats.shape[0] == 0: return
        selected_columns = df_formats.columns[~df_formats.columns.isin([ 'descriptions' ])]
        df_formats = df_formats[selected_columns]
        self.store_append(df=df_formats, name_table='collection_formats')

    def labels(self, df_labels: pd.DataFrame) -> None:
        if df_labels.shape[0] == 0: return
        self.store_append(df=df_labels, name_table='collection_labels')

    def genres(self, df_genres: pd.DataFrame) -> None:
        if df_genres.shape[0] == 0: return
        self.store_append(df=df_genres, name_table='collection_genres')  

    def styles(self, df_styles: pd.DataFrame) -> None:
        if df_styles.shape[0] == 0: return
        self.store_append(df=df_styles, name_table='collection_styles')    

    def release_videos(self, df_videos: pd.DataFrame) -> None:
        if df_videos.shape[0] == 0: return
        selected_columns = df_videos.columns[df_videos.columns.isin(['id_release', 'title', 'uri'])]
        df_videos = df_videos[selected_columns]
        df_videos = df_videos.rename(columns={'uri': 'url_video'})
        self.store_append(df=df_videos, name_table='collection_videos') 
        
    def release_tracks(self, df_tracks: pd.DataFrame) -> None:
        if df_tracks.shape[0] == 0: return
        self.store_append(df=df_tracks, name_table='collection_tracks') 
        
    def release_stats(self, df_release: pd.DataFrame) -> None:
        if df_release.shape[0] == 0: return
        selected_columns = df_release.columns[df_release.columns.isin(['id',\
            'community.have', 'community.want', 'community.rating.count', 'community.rating.average',\
            'date_added', 'date_changed', 'num_for_sale', 'lowest_price', 'released', 'time_retrieved'])]
        df_release = df_release[selected_columns]
        df_release = df_release.rename(columns={'id': 'id_release',\
            'community.have': 'qty_have', 'community.want': 'qty_want', 'community.rating.count': 'qty_ratings', 'community.rating.average': 'avg_rating',\
            'num_for_sale': 'qty_for_sale', 'lowest_price': 'amt_price_lowest', 'released': 'date_released'})
        self.store_append(df=df_release, name_table='collection_stats') 

        
class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def create_tables(self) -> None:
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        if not self.table_exists('artist'):
            sql = "CREATE TABLE artist(name_artist TEXT, id_artist INT, api_artist TEXT,\
                                    url_artist TEXT, url_releases TEXT, text_profile TEXT, data_quality TEXT, not_found INT);"
            cursor.execute(sql)
        if not self.table_exists('artist_aliases'):
            sql = "CREATE TABLE artist_aliases (id_alias INTEGER, name_alias TEXT, api_alias TEXT, id_artist INTEGER);"
            cursor.execute(sql)
        if not self.table_exists('artist_members'):
            sql = "CREATE TABLE artist_members (id_member INTEGER, name_member TEXT, api_member TEXT, is_active INTEGER, id_artist INTEGER)"
            cursor.execute(sql)
        if not self.table_exists('artist_groups'):
            sql = "CREATE TABLE artist_groups (id_group INTEGER, name_group TEXT, api_group TEXT, is_active INTEGER, id_artist INTEGER)"
            cursor.execute(sql)
        if not self.table_exists('artist_images'):    
            sql = "CREATE TABLE artist_images (id_artist INTEGER, type TEXT, url_image TEXT, url_image_150 TEXT, width_image INTEGER, height_image INTEGER)"
            cursor.execute(sql)
        if not self.table_exists('artist_urls'):      
            sql = "CREATE TABLE artist_urls (url_artist TEXT, id_artist INTEGER)"
            cursor.execute(sql)
        if not self.view_exists('vw_artist_edges'):    
            sql = "CREATE VIEW vw_artist_edges AS\
                SELECT id_artist AS id_from, id_alias AS id_to, 'alias' AS type_edge, 1 AS is_active FROM artist_aliases\
                UNION\
                SELECT id_artist AS id_from, id_member AS id_to, 'group_member', is_active FROM artist_members\
                UNION\
                SELECT id_group AS id_from, id_artist AS id_to, 'member_group', is_active FROM artist_groups"
            cursor.execute(sql)       

    def artists(self, df_artists: pd.DataFrame) -> None:
        if df_artists.shape[0] == 0: return
        if not self.table_exists('artist'): self.create_tables()
        selected_columns = df_artists.columns[~df_artists.columns.isin([ 'images', 'members', 'aliases', 'groups', 'realname', 'namevariations',\
            'message', 'urls' ])]
        df_artists = df_artists[selected_columns]
        df_artists = df_artists.rename(columns={'id': 'id_artist', 'name': 'name_artist', 'resource_url': 'api_artist', 'uri': 'url_artist',\
            'releases_url': 'url_releases', 'profile': 'text_profile'})
        self.store_append(df=df_artists, name_table='artist')

    def aliases(self, df_aliases: pd.DataFrame) -> None:
        if df_aliases.shape[0] == 0: return
        if not self.table_exists('artist_aliases'): self.create_tables()
        self.store_append(df=df_aliases, name_table='artist_aliases')

    def members(self, df_members: pd.DataFrame) -> None:
        if df_members.shape[0] == 0: return
        if not self.table_exists('artist_members'): self.create_tables()
        self.store_append(df=df_members, name_table='artist_members')

    def groups(self, df_groups: pd.DataFrame) -> None:
        if df_groups.shape[0] == 0: return
        if not self.table_exists('artist_groups'): self.create_tables()
        self.store_append(df=df_groups, name_table='artist_groups')

    def images(self, df_images: pd.DataFrame) -> None:
        if df_images.shape[0] == 0: return
        if not self.table_exists('artist_images'): self.create_tables()
        self.store_append(df=df_images, name_table='artist_images')

    def urls(self, df_urls: pd.DataFrame) -> None:
        if df_urls.shape[0] == 0: return
        if not self.table_exists('artist_urls'): self.create_tables()
        self.store_append(df=df_urls, name_table='artist_urls')