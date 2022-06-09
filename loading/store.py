import sqlite3
import numpy as np
import pandas as pd

class DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file  

    def store_replace(self, df: pd.DataFrame, name_table: str) -> None:
        db = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db, if_exists='replace', index=False) 
        db.close() 

    def store_append(self, df: pd.DataFrame, name_table: str) -> None:
        db = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db, if_exists='append', index=False) 
        db.close()   

class Collection(DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def write_items(self, df_items: pd.DataFrame) -> None:
        selected_columns = df_items.columns[~df_items.columns.isin([ 'basic_information.thumb', 'basic_information.cover_image',\
            'basic_information.artists', 'basic_information.labels', 'basic_information.formats', 'basic_information.genres','basic_information.styles' ])]
        df_items = df_items[selected_columns]
        df_items = df_items.rename(columns={'id': 'id_release', 'instance_id': 'id_instance',\
            'basic_information.id': 'id_basic_info', 'basic_information.master_id': 'id_master', 'basic_information.master_url': 'url_master',\
            'basic_information.resource_url' : 'api_master'})
        self.store_replace(df=df_items, name_table='collection_items')

    def write_artists(self, df_artists: pd.DataFrame) -> None:
        self.store_replace(df=df_artists, name_table='collection_artists')

    def write_formats(self, df_formats: pd.DataFrame) -> None:
        selected_columns = df_formats.columns[~df_formats.columns.isin([ 'descriptions' ])]
        df_formats = df_formats[selected_columns]
        self.store_replace(df=df_formats, name_table='collection_formats')

    def write_labels(self, df_labels: pd.DataFrame) -> None:
        self.store_replace(df=df_labels, name_table='collection_labels')

    def write_genres(self, df_genres: pd.DataFrame) -> None:
        self.store_replace(df=df_genres, name_table='collection_genres')  

    def write_styles(self, df_styles: pd.DataFrame) -> None:
        self.store_replace(df=df_styles, name_table='collection_styles')    

    def write_lowest_value(self, df_lowest_value: pd.DataFrame) -> None:
        self.store_replace(df=df_lowest_value, name_table='collection_item_value')

    def read_new_artist_id(self) -> pd.DataFrame:
        db = sqlite3.connect(self.db_file)
        cursor = db.cursor()
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='artist' ''')
        has_table = cursor.fetchone()[0]==1 
        if has_table:
            sql_statement = "SELECT DISTINCT collection_artists.id_artist, collection_artists.api_artist \
                             FROM collection_artists \
                             LEFT JOIN artist  ON  artist.id_artist = collection_artists.id_artist \
                             WHERE artist.name_artist IS NULL AND \
                                   collection_artists.api_artist IS NOT NULL AND LENGTH(collection_artists.api_artist) > 0;"
            df_artist = pd.read_sql_query(sql_statement, con=db)
        else:
            df_artist = pd.read_sql(sql="SELECT DISTINCT id_artist, api_artist FROM collection_artists", con=db)
        db.close() 
        return df_artist


class Artists(DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def write_artists(self, df_artists: pd.DataFrame) -> None:
        selected_columns = df_artists.columns[~df_artists.columns.isin([ 'images', 'members', 'aliases', 'groups', 'realname', 'namevariations',\
            'message', 'urls' ])]
        df_artists = df_artists[selected_columns]
        df_artists = df_artists.rename(columns={'id': 'id_artist', 'name': 'name_artist', 'resource_url': 'api_artist', 'uri': 'url_artist',\
            'releases_url': 'url_releases', 'profile': 'text_profile'})
        self.store_append(df=df_artists, name_table='artist')

    def write_images(self, df_images: pd.DataFrame) -> None:
        self.store_append(df=df_images, name_table='artist_images')
                                                             
    def write_groups(self, df_groups: pd.DataFrame) -> None:
        self.store_append(df=df_groups, name_table='artist_groups')

    def write_aliases(self, df_aliases: pd.DataFrame) -> None:
        self.store_append(df=df_aliases, name_table='artist_aliases')

    def write_members(self, df_members: pd.DataFrame) -> None:
        self.store_append(df=df_members, name_table='artist_members')

    def write_urls(self, df_urls: pd.DataFrame) -> None:
        self.store_append(df=df_urls, name_table='artist_urls')