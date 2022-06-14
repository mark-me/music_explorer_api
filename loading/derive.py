from cmath import isnan
import numpy as np
import pandas as pd


class _DeriveBase:

    def __init__(self, df) -> None:
        self.df = df
        self.colname_id = 'id'

    def column_dicts_to_df(self, col_source: str) -> pd.DataFrame:
        lst_dfs = []
        for index, row in self.df.iterrows():
            value = row[col_source]
            if isinstance(value, list):
                df_item = pd.DataFrame(row[col_source])
                df_item[self.colname_id] = np.repeat(row['id'], df_item.shape[0]).tolist()
                lst_dfs.append(df_item)
        df = pd.concat(lst_dfs, ignore_index=True)
        return df

    def column_list_to_df(self, colname:str) -> pd.DataFrame:
        df_expanded = pd.DataFrame(dict(
            id_artist=self.df.id.values.repeat(self.df[colname].str.len()),
        )).join(pd.DataFrame(self.df[colname].sum()))
        return df_expanded


class Collection(_DeriveBase):

    def __init__(self, df_releases) -> None:
        super().__init__(df=df_releases)
        self.colname_id = 'id_release'

    def artists(self) -> pd.DataFrame:
        df_artist = self.column_dicts_to_df(col_source='basic_information.artists')
        df_artist = df_artist.rename(columns={'id': 'id_artist', 'name': 'name_artist', 'resource_url': 'api_artist'})
        df_artist = df_artist[['id_release', 'id_artist', 'name_artist', 'api_artist']]
        return(df_artist)

    def labels(self) -> pd.DataFrame:
        df_label = self.column_dicts_to_df(col_source='basic_information.labels')
        df_label = df_label[['id_release', 'id', 'name', 'resource_url']]
        df_label = df_label.rename(columns={'id': 'id_label', 'name': 'name_label', 'resource_url': 'api_label'})
        return(df_label)

    def formats(self) -> pd.DataFrame:
        df_format = self.column_dicts_to_df(col_source='basic_information.formats')
        df_format = df_format.rename(columns={'name': 'name_format', 'qty': 'qty_format'})
        return(df_format)

    def styles(self) -> pd.DataFrame:
        df_style = self.column_dicts_to_df(col_source='basic_information.styles')
        df_style = df_style.set_axis(['name_style', 'id_release'], axis=1)
        return(df_style)

    def genres(self) -> pd.DataFrame:
        df_genre = self.column_dicts_to_df(col_source='basic_information.genres')
        df_genre = df_genre.set_axis(['name_genre', 'id_release'], axis=1)
        return(df_genre)


class Artists(_DeriveBase):
    def __init__(self, df_artist: pd.DataFrame) -> None:
        super().__init__(df=df_artist)
        self.colname_id = 'id_artist'

    def images(self) -> pd.DataFrame:
        df_image = pd.DataFrame()
        if 'images' in self.df:
            df_image = self.column_dicts_to_df(col_source='images')
            df_image = df_image[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
            df_image = df_image.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150', 'width': 'width_image', 'height': 'height_image'})
        return df_image

    def groups(self) -> pd.DataFrame:
        df_group = pd.DataFrame()
        if 'groups' in self.df:
            df_group = self.column_dicts_to_df(col_source='groups')
            df_group = df_group.rename(columns={'id': 'id_group', 'name': 'name_group', 'resource_url': 'api_group', 'active': 'is_active',\
                'thumbnail_url': 'url_thumbnail'})
        return df_group

    def aliases(self) -> pd.DataFrame:
        df_alias = pd.DataFrame()
        if 'aliases' in self.df:
            df_alias = self.column_dicts_to_df(col_source='aliases')
            df_alias = df_alias.rename(columns={'id': 'id_alias', 'name': 'name_alias', 'resource_url': 'api_alias', 'thumbnail_url': 'url_thumbnail'})
        return df_alias

    def members(self) -> pd.DataFrame:
        df_member = pd.DataFrame()
        if 'members' in self.df:
            df_member = self.column_dicts_to_df(col_source='members')
            df_member = df_member.rename(columns={'id': 'id_member', 'name': 'name_member', 'resource_url': 'api_member', 'active': 'is_active',\
                'height': 'height_image', 'thumbnail_url': 'url_thumbnail'})
        return df_member

    def urls(self) -> pd.DataFrame:
        df_url = pd.DataFrame()
        if 'urls' in self.df:
            df_url = self.column_dicts_to_df(col_source='urls')
            df_url = df_url.set_axis(['url_artist', 'id_artist'], axis=1)
        return df_url
    
    
class Release(_DeriveBase):
    def __init__(self, df_release: pd.DataFrame) -> None:
        super().__init__(df=df_release)
        self.colname_id = 'id_release'
        
    def videos(self) -> pd.DataFrame:
        df_videos = pd.DataFrame()
        if 'videos' in self.df:
            df_videos = self.column_dicts_to_df(col_source='videos')
            df_videos = df_videos[['id_release', 'title', 'uri']]
        return df_videos
    
    def tracks(self) -> pd.DataFrame:
        df_tracks = pd.DataFrame()
        if 'tracklist' in self.df:
            df_tracks = self.column_dicts_to_df(col_source='tracklist')
            df_tracks = df_tracks[['id_release', 'position', 'title', 'duration']]
        return df_tracks
            