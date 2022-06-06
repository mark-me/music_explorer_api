import sqlite3
import numpy as np
import pandas as pd

class DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file  

    def store_replace(self, df: pd.DataFrame, name_table: str) -> None:
        db = sqlite3.connect(self.db_file)
        df.to_sql(name=name_table, con=db, if_exists='replace') 
        db.close() 

class Collection(DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def write_items(self, df_collection_items: pd.DataFrame) -> None:
        selected_columns = df_collection_items.columns[~df_collection_items.columns.isin([ "basic_information.thumb", "basic_information.cover_image",\
            "basic_information.artists", "basic_information.labels", "basic_information.formats", "basic_information.genres","basic_information.styles" ])]
        df_collection_items = df_collection_items[selected_columns]
        self.store_replace(df=df_collection_items, name_table="collection_items")

    def write_artists(self, df_collection_artists: pd.DataFrame) -> None:
        self.store_replace(df=df_collection_artists, name_table="collection_artists")

    def write_formats(self, df_collection_formats: pd.DataFrame) -> None:
        selected_columns = df_collection_formats.columns[~df_collection_formats.columns.isin([ "descriptions" ])]
        df_collection_formats = df_collection_formats[selected_columns]
        self.store_replace(df=df_collection_formats, name_table="collection_formats")

    def write_labels(self, df_collection_labels: pd.DataFrame) -> None:
        self.store_replace(df=df_collection_labels, name_table="collection_labels")

    def write_genres(self, df_collection_genres: pd.DataFrame) -> None:
        self.store_replace(df=df_collection_genres, name_table="collection_genres")  

    def write_styles(self, df_collection_styles: pd.DataFrame) -> None:
        self.store_replace(df_collection_styles, name_table="collection_styles")    

    def write_lowest_value(self, df_lowest_value: pd.DataFrame) -> None:
        self.store_replace(df_lowest_value, name_table="collection_item_value")

class Artists(DBStorage):

    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def write_artists(self, df_artists: pd.DataFrame) -> None:
        pass

    def write_images(self, df_artist_images: pd.DataFrame) -> None:
        pass
                                                             
    def write_groups(self, df_artist_groups: pd.DataFrame) -> None:
        pass

    def write_aliases(self, df_artist_aliases: pd.DataFrame) -> None:
        pass

    def write_members(self, df_artist_members: pd.DataFrame) -> None:
        pass

    def write_urls(self, df_artist_urls: pd.DataFrame) -> None:
        pass