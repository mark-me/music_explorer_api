import numpy as np
import pandas as pd


class Collection:

    def __init__(self, df_collection_items) -> None:
        self.df_collection_items = df_collection_items

    def artists(self) -> pd.DataFrame:
        pass


    def labels(self) -> pd.DataFrame:
        pass


    def formats(self) -> pd.DataFrame:
        pass


    def styles(self) -> pd.DataFrame:
        pass


    def genres(self) -> pd.DataFrame:
        pass


class Artists:

    def __init__(self, df_artists: pd.DataFrame) -> None:
        self.df_artists = df_artists

    def images(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        pass

    def groups(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        pass

    def aliases(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        pass

    def members(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        pass