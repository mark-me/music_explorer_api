from cmath import isnan
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
        df_label = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.labels'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.labels'].sum()))

        df_label = df_label[['id_release', 'id', 'name', 'resource_url']]
        df_label = df_label.rename(columns={'id': 'id_label', 'name': 'name_label', 'resource_url': 'api_label'})
        df_label = df_label.drop_duplicates(subset=['id_release', 'id_label'])
        return(df_label)

    def formats(self) -> pd.DataFrame:
        df_format = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.formats'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.formats'].sum()))
        df_format = df_format.rename(columns={'name': 'name_format', 'qty': 'qty_format'})
        return(df_format)

    def styles(self) -> pd.DataFrame:
        df_style = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.styles'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.styles'].sum()))
        df_style = df_style.set_axis(['id_release', 'name_style'], axis=1)
        return(df_style)

    def genres(self) -> pd.DataFrame:
        df_genre = pd.DataFrame(dict(
            id_release=self.df_releases.id.values.repeat(self.df_releases['basic_information.genres'].str.len()),
        )).join(pd.DataFrame(self.df_releases['basic_information.genres'].sum()))
        df_genre = df_genre.set_axis(['id_release', 'name_genre'], axis=1)
        return(df_genre)


class Artists:

    def __init__(self, df_artist: pd.DataFrame) -> None:
        self.df_artist = df_artist

    def __column_dicts_to_df(self, colname: str) -> pd.DataFrame:
        lst_dfs = []
        for index, row in self.df_artist.iterrows():
            if not pd.isnull(row[colname]):
                df_item = pd.DataFrame(row[colname])
                df_item['id_artist'] = np.repeat(row['id'], df_item.shape[0]).tolist()
                lst_dfs.append(df_item)
        df = pd.concat(lst_dfs, ignore_index=True)
        return df

    def images(self) -> pd.DataFrame:
        colname = 'images'
        df_image = pd.DataFrame(dict(
            id_artist=self.df_artist.id.values.repeat(self.df_artist[colname].str.len()),
        )).join(pd.DataFrame(self.df_artist[colname].sum()))
        df_image = df_image[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
        df_image = df_image.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150', 'width': 'width_image', 'height': 'height_image'})
        return df_image

    def groups(self) -> pd.DataFrame:
        # res = self.df_artist[['id', 'groups']].explode('groups').reset_index(drop=True) # For list
        df_group = self.__column_dicts_to_df(colname='groups')
        df_group = df_group.rename(columns={'id': 'id_group', 'name': 'name_group', 'resource_url': 'api_group', 'active': 'is_active',\
            'thumbnail_url': 'url_thumbnail'})
        return df_group

    def aliases(self) -> pd.DataFrame:
        df_alias = self.__column_dicts_to_df(colname='aliases')
        df_alias = df_alias.rename(columns={'id': 'id_alias', 'name': 'name_alias', 'resource_url': 'api_alias', 'thumbnail_url': 'url_thumbnail'})
        return df_alias

    def members(self) -> pd.DataFrame:
        df_member = self.__column_dicts_to_df(colname='members')
        df_member = df_member[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
        df_member = df_member.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150', 'width': 'width_image', 'height': 'height_image'})
        return df_member

    def urls(self) -> pd.DataFrame:
        df_url = pd.DataFrame(dict(
            id_artist=self.df_artist.id.values.repeat(self.df_artist['urls'].str.len()),
        )).join(pd.DataFrame(self.df_artist['urls'].sum()))
        df_url = df_url[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
        df_url = df_url.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150', 'width': 'width_image', 'height': 'height_image'})
        return df_url     