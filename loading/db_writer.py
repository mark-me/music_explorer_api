import sqlite3
import numpy as np
import pandas as pd

class _DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file  

    def drop_existing_table(self, name_table: str) -> None:
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
        
    def items(self, df_items: pd.DataFrame) -> None:
        if df_items.shape[0] == 0: return
        selected_columns = df_items.columns[~df_items.columns.isin([ 'basic_information.thumb', 'basic_information.cover_image',\
            'basic_information.artists', 'basic_information.labels', 'basic_information.formats', 'basic_information.genres','basic_information.styles' ])]
        df_items = df_items[selected_columns]
        df_items = df_items.rename(columns={'id': 'id_release', 'instance_id': 'id_instance',\
            'basic_information.id': 'id_basic_info', 'basic_information.master_id': 'id_master', 'basic_information.master_url': 'url_master',\
            'basic_information.resource_url' : 'api_master'})
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

    def lowest_value(self, df_lowest_value: pd.DataFrame) -> None:
        if df_lowest_value.shape[0] == 0: return
        self.store_append(df=df_lowest_value, name_table='collection_item_value')


class Artists(_DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def artists(self, df_artists: pd.DataFrame) -> None:
        if df_artists.shape[0] == 0: return
        selected_columns = df_artists.columns[~df_artists.columns.isin([ 'images', 'members', 'aliases', 'groups', 'realname', 'namevariations',\
            'message', 'urls' ])]
        df_artists = df_artists[selected_columns]
        df_artists = df_artists.rename(columns={'id': 'id_artist', 'name': 'name_artist', 'resource_url': 'api_artist', 'uri': 'url_artist',\
            'releases_url': 'url_releases', 'profile': 'text_profile'})
        self.store_append(df=df_artists, name_table='artist')

    def images(self, df_images: pd.DataFrame) -> None:
        if df_images.shape[0] == 0: return
        self.store_append(df=df_images, name_table='artist_images')
                                                             
    def groups(self, df_groups: pd.DataFrame) -> None:
        if df_groups.shape[0] == 0: return
        self.store_append(df=df_groups, name_table='artist_groups')

    def aliases(self, df_aliases: pd.DataFrame) -> None:
        if df_aliases.shape[0] == 0: return
        self.store_append(df=df_aliases, name_table='artist_aliases')

    def members(self, df_members: pd.DataFrame) -> None:
        if df_members.shape[0] == 0: return
        self.store_append(df=df_members, name_table='artist_members')

    def urls(self, df_urls: pd.DataFrame) -> None:
        if df_urls.shape[0] == 0: return
        self.store_append(df=df_urls, name_table='artist_urls')