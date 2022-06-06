import sqlite3
import numpy as np
import pandas as pd


class Collection():

    def __init__(self, db_file) -> None:
        self.db_file = db_file

    def items(self, df_collection_items: pd.DataFrame) -> None:

        selected_columns = df_collection_items.columns[~df_collection_items.columns.isin([ "basic_information.thumb", "basic_information.cover_image",\
                                                                               "basic_information.artists", "basic_information.labels",\
                                                                               "basic_information.formats", "basic_information.genres",\
                                                                               "basic_information.styles" ])]
        df_collection_items = df_collection_items[selected_columns]

        db = sqlite3.connect(self.db_file)
        df_collection_items.to_sql(name="collection_items", con=db, if_exists='replace')
        db.close()

    def artists(self, df_collection_artists: pd.DataFrame) -> None:
        pass

    def formats(self, df_collection_formats: pd.DataFrame) -> None:
        pass

    def labels(self, df_collection_labels: pd.DataFrame) -> None:
        pass

    def genres(self, df_collection_genres: pd.DataFrame) -> None:
        pass    

    def styles(self, df_collection_styles: pd.DataFrame) -> None:
        pass   

class Artists():

    def __init__(self, db_file) -> None:
        self.db_file = db_file    

    def artists(self, df_artists: pd.DataFrame) -> None:
        pass

    def images(self, df_artist_images: pd.DataFrame) -> None:
        pass
                                                             
    def groups(self, df_artist_groups: pd.DataFrame) -> None:
        pass

    def aliases(self, df_artist_aliases: pd.DataFrame) -> None:
        pass

    def members(self, df_artist_members: pd.DataFrame) -> None:
        pass

    def urls(self, df_artist_urls: pd.DataFrame) -> None:
        pass