import numpy as np
import pandas as pd


class Collection:

    def __init__(self, df_releases) -> None:
        self.df_releases = df_releases

    def artists(self) -> pd.DataFrame:
        df_artist = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.artists'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.artists'].sum()))

        df_artist = df_artist[['id_release', 'id', 'name', 'resource_url']]
        df_artist = df_artist.rename(columns={'id': 'id_artist', 'name': 'name_artist', 'resource_url': 'api_artist'})
        df_artist = df_artist.drop_duplicates(subset=['id_release', 'id_artist'])
        return(df_artist)

    def labels(self) -> pd.DataFrame:
        df_labels = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.labels'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.labels'].sum()))

        df_labels = df_labels[['id_release', 'id', 'name', 'resource_url']]
        df_labels = df_labels.rename(columns={'id': 'id_label', 'name': 'name_label', 'resource_url': 'api_label'})
        df_labels = df_labels.drop_duplicates(subset=['id_release', 'id_label'])
        return(df_labels)

    def formats(self) -> pd.DataFrame:
        df_formats = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.formats'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.formats'].sum()))
        df_formats = df_formats.rename(columns={'name': 'name_format', 'qty': 'qty_format'})
        return(df_formats)

    def styles(self) -> pd.DataFrame:
        df_styles = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.styles'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.styles'].sum()))
        df_styles = df_styles.set_axis(['id_release', 'name_style'], axis=1)
        return(df_styles)

    def genres(self) -> pd.DataFrame:
        df_genres = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.genres'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.genres'].sum()))
        df_genres = df_genres.set_axis(['id_release', 'name_genre'], axis=1)
        return(df_genres)


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