from cmath import isnan
import datetime as dt
import numpy as np
import pandas as pd
import igraph as igraph
import discogs_client
from tqdm import tqdm

import db_writer as _db_writer


class Artists():
    """A class that processes artist related data
    """
    def __init__(self, artist: discogs_client.Artist, db_file: str) -> None:
        self.__artists = artist
        self.db_writer = _db_writer.Artists(db_file=db_file)

    def process(self) -> None:
        for artist in tqdm(self.__artists, total = len(self.__artists)):
            exists = self.db_writer.in_db(id_artist=artist.id)
            if not exists:
                self.__artist(artist=artist)
                self.__images(artist=artist)
                self.__aliases(artist=artist)
                self.__groups(artist=artist)
                self.__members(artist=artist)
                self.__urls(artist=artist)

    def __artist(self, artist: discogs_client.Artist) -> None:
        try:
            dict_artist = {'id_artist': artist.id, 'name_artist': artist.name}
            df_artist = pd.DataFrame([dict_artist])
            self.db_writer.artists(df_artists=df_artist)
        except:
            pass

    def __images(self, artist: discogs_client.Artist) -> None:
        try:
            lst_images = []
            for image in artist.images:
                df_image = pd.DataFrame([image])
                lst_images.append(df_image)
            if len(lst_images) > 0:
                df_images = pd.concat(lst_images, axis=0, ignore_index=True)
                df_images['id_artist'] = artist.id
                df_images = df_images[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
                df_images = df_images.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150',\
                                                      'width': 'width_image', 'height': 'height_image'})
                self.db_writer.images(df_images=df_images)
        except:
            pass

    def __groups(self, artist: discogs_client.Artist) -> None:
        try:
            lst_groups = []
            for group in artist.groups:
                df_group = pd.DataFrame([group.data])
                lst_groups.append(df_group)
            if len(lst_groups) > 0:
                df_groups = pd.concat(lst_groups, axis=0, ignore_index=True)
                df_groups['id_artist'] = artist.id
                df_groups = df_groups.rename(columns={'id': 'id_group', 'name': 'name_group', 'resource_url': 'api_group',\
                    'active': 'is_active', 'thumbnail_url': 'url_thumbnail'})
                self.db_writer.groups(df_groups=df_groups)
        except:
            pass

    def __aliases(self, artist: discogs_client.Artist) -> None:
        try:
            lst_aliases = []
            for alias in artist.aliases:
                df_alias = pd.DataFrame([alias.data])
                lst_aliases.append(df_alias)
            if len(lst_aliases) > 0:
                df_aliases = pd.concat(lst_aliases, axis=0, ignore_index=True)
                df_aliases['id_artist'] = artist.id
                df_aliases = df_aliases.rename(columns={'id': 'id_alias', 'name': 'name_alias',\
                    'resource_url': 'api_alias', 'thumbnail_url': 'url_thumbnail'})
                self.db_writer.aliases(df_aliases=df_aliases)
        except:
            pass

    def __members(self, artist: discogs_client.Artist) -> None:
        try:
            lst_members = []
            for member in artist.members:
                df_member = pd.DataFrame([member.data])
                lst_members.append(df_member)
            if len(lst_members) > 0:
                df_members = pd.concat(lst_members, axis=0, ignore_index=True)
                df_members['id_artist'] = artist.id
                df_members = df_members.rename(columns={'id': 'id_member', 'name': 'name_member', 'resource_url': 'api_member',\
                    'active': 'is_active', 'height': 'height_image', 'thumbnail_url': 'url_thumbnail'})
                self.db_writer.members(df_members=df_members)
        except:
            pass

    def __urls(self, artist: discogs_client.Artist) -> pd.DataFrame:
        try:
            lst_urls = []
            for url in artist.urls:
                df_url = pd.DataFrame([url])
                lst_urls.append(df_url)
            if len(lst_urls) > 0:
                df_urls = pd.concat(lst_urls, axis=0, ignore_index=True)
                df_urls['id_artist'] = artist.id
                df_urls = df_urls.set_axis(['url_artist', 'id_artist'], axis=1)
                self.db_writer.urls(df_urls=df_urls)
        except:
            pass

class Release():
    """A class that processes release related data
    """
    def __init__(self, release: discogs_client.Release, db_file: str) -> None:
        self.__discogs_release = release
        self.__artists = Artists(artist=release.artists, db_file=db_file)
        self.db_writer = _db_writer.Release(db_file=db_file)

    def process(self) -> None:
        self.__stats()
        exists = self.db_writer.in_db(id_release=self.__discogs_release.id)
        if not exists:
            self.__release()
            self.__labels()
            self.__formats()
            self.__genres()
            self.__styles()
            self.__credits()
            self.__tracks()
            self.__track_artists()
            self.__videos()
            self.__release_artists()
            self.__artists.process()

    def __release(self) -> None:
        df_release = pd.DataFrame([self.__discogs_release.data])
        cols_release = ['id', 'master_id', 'title', 'thumb', 'cover_image', 'year', 'uri', 'country', 'released']
        cols_df = df_release.columns.values.tolist()
        cols = list(set(cols_release) & set(cols_df))
        df_release = df_release[cols]
        df_release = df_release.rename(columns={'id': 'id_release', 'master_id': 'id_master', 'cover_image': 'url_cover',\
            'thumb': 'url_thumbnail', 'uri': 'url_release', 'released': 'date_released'})
        self.db_writer.release(df_release=df_release)

    def __release_artists(self) -> None:
        lst_artists = []
        for artist in self.__discogs_release.artists:
            lst_artists = {'id_artist': artist.id, 'id_release': self.__discogs_release.id}
        df_artists = pd.DataFrame[lst_artists]
        self.db_writer.artists(df_artists=df_artists)

    def __labels(self) -> None:
        lst_labels = []
        for label in self.__discogs_release.labels:
            df_label = pd.DataFrame([label.data])
            lst_labels.append(df_label)
        df_labels = pd.DataFrame()
        if len(lst_labels) > 0:
            df_labels = pd.concat(lst_labels, axis=0, ignore_index=True)
            df_labels['id_release'] = self.__discogs_release.id
            df_labels = df_labels[['id_release', 'id', 'name', 'catno', 'thumbnail_url']]
            df_labels = df_label.rename(columns={'id': 'id_label', 'name': 'name_label', 'thumbnail_url': 'url_thumbnail'})
            self.db_writer.labels(df_labels=df_label)

    def __formats(self) -> None:
        lst_formats = []
        for format in self.__discogs_release.formats:
            df_format = pd.DataFrame([format])
            lst_formats.append(df_format)
        df_formats = pd.DataFrame()
        if len(lst_formats) > 0:
            df_formats = pd.concat(lst_formats, axis=0, ignore_index=True)
            df_formats = df_formats[['name', 'qty']]
            df_formats['id_release'] = self.__discogs_release.id
            df_formats = df_formats.rename(columns={'name': 'name_format', 'qty': 'qty_format'})
            self.db_writer.formats(df_formats=df_formats)

    def __styles(self) -> None:
        lst_styles = []
        for style in self.__discogs_release.styles:
            df_style = pd.DataFrame([style])
            lst_styles.append(df_style)
        df_styles = pd.DataFrame()
        if len(lst_styles) > 0:
            df_styles = pd.concat(lst_styles, axis=0, ignore_index=True)
            df_styles['id_release'] = self.__discogs_release.id
            self.db_writer.styles(df_styles=df_styles)

    def __genres(self) -> None:
        lst_genres = []
        for genre in self.__discogs_release.genres:
            df_genre = pd.DataFrame([genre])
            lst_genres.append(df_genre)
        if len(lst_genres) > 0:
            df_genres = pd.concat(lst_genres, axis=0, ignore_index=True)
            df_genres['id_release'] = self.__discogs_release.id
            df_genres = df_genres.set_axis(['name_genre', 'id_release'], axis=1)
            self.db_writer.genres(df_genres=df_genres)

    def __credits(self) -> None:
        lst_artist = []
        for artist in self.__discogs_release.credits:
            df_artist = pd.DataFrame([artist.data])
            lst_artist.append(df_artist)
        if len(lst_artist) > 0:
            df_artists = pd.concat(lst_artist, axis=0, ignore_index=True)
            df_artists = df_artists[['name', 'role', 'id', 'resource_url', 'thumbnail_url']]  
            df_artists = df_artists.rename(columns={'name': 'name_artist', 'id': 'id_artist', 'resource_url': 'api_artist',\
                'thumbnail_url': 'url_thumbnail'})      
            df_artists['id_release'] = self.__discogs_release.id 
            self.db_writer.credits(df_credits=df_artists)
    
    def __tracks(self) -> None:
        lst_tracks = []
        for track in self.__discogs_release.tracklist:
            dict_track = track.data
            lst_tracks.append(pd.DataFrame([dict_track]))
        if len(lst_tracks) > 0:
            df_tracks = pd.concat(lst_tracks, axis=0, ignore_index=True)
            df_tracks = df_tracks[['position', 'title', 'duration']]
            df_tracks['id_release'] = self.__discogs_release.id
            self.db_writer.tracks(df_tracks=df_tracks)
    
    def __track_artists(self) -> None:
        lst_artist = []
        for track in self.__discogs_release.tracklist:
            if 'extraartists' in track.data:
                dict_artist = track.data['extraartists']
                df_artist = pd.DataFrame(dict_artist)
                df_artist['position'] = track.data['position']
                lst_artist.append(df_artist)
        if len(lst_artist) > 0:
            df_artists = pd.concat(lst_artist, axis=0, ignore_index=True)  
            df_artists = df_artists[['name', 'role', 'id', 'resource_url', 'thumbnail_url', 'position']]  
            df_artists = df_artists.rename(columns={'name': 'name_artist', 'id': 'id_artist', 'resource_url': 'api_artist',\
                'thumbnail_url': 'url_thumbnail'})
            df_artists['id_release'] = self.__discogs_release.id 
            self.db_writer.track_artist(df_artists=df_artists)
    
    def __videos(self) -> None:
        lst_video = []
        for video in self.__discogs_release.videos:
            dict_video = video.data
            lst_video.append(pd.DataFrame([dict_video]))
        if len(lst_video) > 0:
            df_videos = pd.concat(lst_video, axis=0, ignore_index=True)
            df_videos = df_videos[['uri', 'title', 'duration']]
            df_videos = df_videos.rename(columns={'uri': 'url_video'})
            df_videos['id_release'] = self.__discogs_release.id
            self.db_writer.videos(df_videos=df_videos)
            
    def __stats(self) -> None:
        df_stats = pd.DataFrame([self.__discogs_release.data])
        df_stats = df_stats[['id', 'num_for_sale', 'lowest_price']]
        df_stats['time_value_retrieved'] = dt.datetime.now()
        df_stats = df_stats.rename(columns={'id': 'id_release', 'num_for_sale': 'qty_for_sale', 'lowest_price': 'amt_price_lowest'})        
        dict_community = {key: self.__discogs_release.data['community'][key] for key in ['have', 'want']}
        df_community = pd.DataFrame([dict_community])
        df_community = df_community.rename(columns={'have': 'qty_has', 'want': 'qty_want'}) 
        df_stats = pd.concat([df_stats, df_community], axis=1, join='inner')
        self.db_writer.stats(df_stats=df_stats)


class Collection():
    def __init__(self, db_file: str) -> None:
        self.db_writer = _db_writer.Collection(db_file=db_file)
        self.db_writer.drop_tables()

    def value(self, user: discogs_client.User) -> None:
        df_value = pd.DataFrame([user.collection_value.data])
        df_value['qty_items'] = user.num_collection
        df_value['time_value_retrieved'] = dt.datetime.now()
        self.db_writer.value(df_value=df_value)

class CollectionItem():
    def __init__(self, item: discogs_client.CollectionItemInstance, db_file: str) -> None:
        self.__item = item
        self.__release = Release(release=item.release, db_file=db_file)
        self.db_writer = _db_writer.Collection(db_file=db_file)

    def process(self) -> None:
        self.__collection_item()
        self.__release.process()

    def __collection_item(self) -> None:
        dict_item = {'id_release': self.__item.id,
                     'date_added': self.__item.date_added,
                     'id_instance': self.__item.instance_id,
                     'title': self.__item.data['basic_information']['title'],
                     'id_master': self.__item.data['basic_information']['master_id'],
                     'api_master': self.__item.data['basic_information']['master_url'],
                     'api_release': self.__item.data['basic_information']['resource_url'],
                     'url_thumbnail': self.__item.data['basic_information']['thumb'],
                     'url_cover': self.__item.data['basic_information']['cover_image'],
                     'year_released': self.__item.data['basic_information']['year'],
                     'rating': self.__item.rating,
                     'country': self.__item.release.country}
        df_item = pd.DataFrame(dict_item, index=[0])
        self.db_writer.items(df_items=df_item)


class ArtistNetwork():
    def __init__(self, df_vertices: pd.DataFrame, df_edges: pd.DataFrame) -> None:
        self.df_vertices = df_vertices
        self.df_edges = df_edges
        
    def cluster_betweenness(self) -> pd.DataFrame:
        df_hierarchy = pd.DataFrame()
        return df_hierarchy
    
    def centrality(self) -> pd.DataFrame:
        df_centrality = pd.DataFrame()
        return df_centrality    
    