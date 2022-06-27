import datetime as dt
import numpy as np
import pandas as pd
from cmath import isnan
from tqdm import tqdm
import igraph as igraph
import discogs_client

import db_writer as _db_writer


class Artists():
    """A class that processes artist related data
    """
    def __init__(self, artist: discogs_client.Artist, db_file: str) -> None:
        self.__d_artists = artist
        self.db_file = db_file
        self.process_masters = True

    def process(self) -> None:
        db_writer = _db_writer.Artists(db_file=self.db_file)        
        for artist in tqdm(self.__d_artists, total=len(self.__d_artists), desc="Artists"):
            exists = db_writer.in_db(id_artist=artist.id)
            if not exists:
                df_artists = self.artist(artist=artist)
                if self.process_masters: df_masters = self.masters(artist=artist)
                df_images = self.images(artist=artist)
                df_aliases = self.aliases(artist=artist)
                df_groups = self.groups(artist=artist)
                df_members = self.members(artist=artist)
                df_urls = self.urls(artist=artist)
                db_writer.artists(df_artists=df_artists)
                if self.process_masters: db_writer.masters(df_masters=df_masters)
                db_writer.members(df_members=df_members)
                db_writer.groups(df_groups=df_groups)
                db_writer.aliases(df_aliases=df_aliases)
                db_writer.images(df_images=df_images)
                db_writer.urls(df_urls=df_urls)

    def artist(self, artist: discogs_client.Artist) -> pd.DataFrame:
        df_artist = pd.DataFrame()
        try:
            dict_artist = {'id_artist': artist.id, 'name_artist': artist.name}
            df_artist = pd.DataFrame([dict_artist])
        except:
            pass
        return df_artist

    def masters(self, artist: discogs_client.Artist) -> pd.DataFrame:
        lst_masters = []
        df_masters = pd.DataFrame()
        try:
            qty_pages = artist.releases.pages
            for page_no in tqdm(range(1, qty_pages), total=qty_pages-1, desc=artist.name + " - Masters"):
                page = artist.releases.page(page_no)
                lst_masters = lst_masters + [master.data for master in page]
            df_masters = pd.DataFrame(lst_masters)
            if not df_masters.empty:
                df_masters['id_artist'] = artist.id
                df_masters = df_masters[['id_artist', 'id', 'title', 'type', 'main_release', 'artist', 'role', 'year', 'thumb']]
                df_masters = df_masters.rename(columns={'id': 'id_master', 'main_release': 'id_main_release',\
                                                            'artist': 'name_artist', 'thumb': 'url_thumb'})
        except:
            pass
        return df_masters

    def images(self, artist: discogs_client.Artist) -> pd.DataFrame:
        try:
            lst_images = []
            df_images = pd.DataFrame()
            for image in artist.images:
                df_image = pd.DataFrame([image])
                lst_images.append(df_image)
            if len(lst_images) > 0:
                df_images = pd.concat(lst_images, axis=0, ignore_index=True)
                df_images['id_artist'] = artist.id
                df_images = df_images[['id_artist', 'type', 'uri', 'uri150', 'width', 'height']]
                df_images = df_images.rename(columns={'uri': 'url_image', 'uri150': 'url_image_150',\
                                                      'width': 'width_image', 'height': 'height_image'})
        except:
            return df_images
        return df_images        

    def groups(self, artist: discogs_client.Artist) -> pd.DataFrame:
        lst_groups = []
        df_groups = pd.DataFrame()
        try:
            for group in artist.groups:
                df_group = pd.DataFrame([group.data])
                lst_groups.append(df_group)
            if len(lst_groups) > 0:
                df_groups = pd.concat(lst_groups, axis=0, ignore_index=True)
                df_groups['id_artist'] = artist.id
                df_groups = df_groups.rename(columns={'id': 'id_group', 'name': 'name_group', 'resource_url': 'api_group',\
                    'active': 'is_active', 'thumbnail_url': 'url_thumbnail'})
        except:
            pass
        return df_groups

    def aliases(self, artist: discogs_client.Artist) -> pd.DataFrame:
        lst_aliases = []
        df_aliases = pd.DataFrame()
        try:
            for alias in artist.aliases:
                df_alias = pd.DataFrame([alias.data])
                lst_aliases.append(df_alias)
            if len(lst_aliases) > 0:
                df_aliases = pd.concat(lst_aliases, axis=0, ignore_index=True)
                df_aliases['id_artist'] = artist.id
                df_aliases = df_aliases.rename(columns={'id': 'id_alias', 'name': 'name_alias',\
                    'resource_url': 'api_alias', 'thumbnail_url': 'url_thumbnail'})
        except:
            pass
        return df_aliases

    def members(self, artist: discogs_client.Artist) -> pd.DataFrame:
        lst_members = []
        df_members = pd.DataFrame()
        try:
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
        return df_members

    def urls(self, artist: discogs_client.Artist) -> pd.DataFrame:
        lst_urls = []
        df_urls = pd.DataFrame()
        try:
            for url in artist.urls:
                df_url = pd.DataFrame([url])
                lst_urls.append(df_url)
            if len(lst_urls) > 0:
                df_urls = pd.concat(lst_urls, axis=0, ignore_index=True)
                df_urls['id_artist'] = artist.id
                df_urls = df_urls.set_axis(['url_artist', 'id_artist'], axis=1)
        except:
            pass
        return df_urls


class MasterReleases():
    """A class that processes a list of release related data
    """
    def __init__(self, df_masters: pd.DataFrame, db_file: str) -> None:
        self.__df_masters = df_masters
        self.__db_file = db_file
        
    def process(self) -> None:
        for d_master in tqdm(self.__lst_masters, total=len(self.__lst_masters)):
            master = MasterRelease(release=d_master, db_file=self.__db_file)
            master.process()


class MasterRelease():
    def __init__(self, release, db_file: str) -> None:
        self.d_release = release
        self.db_file = db_file

    def process(self) -> None:
        db_writer = _db_writer.Master(db_file=self.db_file)
        df_stats = self.stats()
        exists = self.db_writer.in_db(id_master=self.d_release.id)
        if not exists:
            df_master = self.master()
            df_genres = self.genres()
            df_styles = self.styles()
            df_tracks = self.tracks()
            df_track_artists = self.track_artists()
            df_videos = self.videos()
            db_writer.master(df_master=df_master)
            db_writer.stats(df_stats=df_stats)
            db_writer.styles(df_styles=df_styles)
            db_writer.genres(df_genres=df_genres)
            db_writer.tracks(df_tracks=df_tracks)
            db_writer.track_artist(df_track_artists=df_track_artists)
            db_writer.videos(df_videos=df_videos)

    def master(self) -> pd.DataFrame:
        pass

    def stats(self) -> pd.DataFrame:
        stats = self.d_release.data['stats']
        dict_stats = {'id_master': self.d_release.id,
                      'qty_wants': stats['community']['in_wantlist'],
                      'qty_has': stats['community']['in_collection']}
        df_stats = pd.DataFrame([dict_stats])
        return df_stats

    def styles(self) -> pd.DataFrame:
        lst_styles = []
        df_styles = pd.DataFrame()
        if not self.d_release.styles is None:
            for style in self.d_release.styles:
                df_style = pd.DataFrame([style])
                lst_styles.append(df_style)
            df_styles = pd.DataFrame()
            if len(lst_styles) > 0:
                df_styles = pd.concat(lst_styles, axis=0, ignore_index=True)
                df_styles['id_release'] = self.d_release.id
        return df_styles

    def genres(self) -> pd.DataFrame:
        lst_genres = []
        df_genres = pd.DataFrame()
        for genre in self.d_release.genres:
            df_genre = pd.DataFrame([genre])
            lst_genres.append(df_genre)
        if len(lst_genres) > 0:
            df_genres = pd.concat(lst_genres, axis=0, ignore_index=True)
            df_genres['id_release'] = self.d_release.id
            df_genres = df_genres.set_axis(['name_genre', 'id_release'], axis=1)
        return df_genres

    def tracks(self) -> pd.DataFrame:
        lst_tracks = []
        df_tracks = pd.DataFrame()
        for track in self.d_release.tracklist:
            dict_track = track.data
            lst_tracks.append(pd.DataFrame([dict_track]))
        if len(lst_tracks) > 0:
            df_tracks = pd.concat(lst_tracks, axis=0, ignore_index=True)
            df_tracks = df_tracks[['position', 'title', 'duration']]
            df_tracks['id_release'] = self.d_release.id
        return df_tracks

    def track_artists(self) -> pd.DataFrame:
        lst_artist = []
        df_artists = pd.DataFrame()
        for track in self.d_release.tracklist:
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
            df_artists['id_release'] = self.d_release.id 
        return df_artists

    def videos(self) -> pd.DataFrame:
        lst_video = []
        df_videos = pd.DataFrame()
        for video in self.d_release.videos:
            dict_video = video.data
            lst_video.append(pd.DataFrame([dict_video]))
        if len(lst_video) > 0:
            df_videos = pd.concat(lst_video, axis=0, ignore_index=True)
            df_videos = df_videos[['uri', 'title', 'duration']]
            df_videos = df_videos.rename(columns={'uri': 'url_video'})
            df_videos['id_release'] = self.d_release.id
        return df_videos


class Release(MasterRelease):
    """A class that processes release related data
    """
    def __init__(self, release, db_file: str) -> None:
        super().__init__(release, db_file)
        self.__artists = Artists(artist=release.artists, db_file=db_file)

    def process(self) -> None:
        db_writer = _db_writer.Release(db_file=self.db_file)
        df_stats = self.stats()
        exists = db_writer.in_db(id_release=self.d_release.id)
        if not exists:
            df_release = self.release()
            df_labels = self.labels()
            df_formats = self.formats()
            df_genres = self.genres()
            df_styles = self.styles()
            df_credits = self.credits()
            df_tracks = self.tracks()
            df_track_artists = self.track_artists()
            df_videos = self.videos()
            df_release_artists = self.release_artists()
            db_writer.release(df_release=df_release)
            db_writer.artists(df_artists=df_release_artists)
            db_writer.labels(df_labels=df_labels)
            db_writer.formats(df_formats=df_formats)
            db_writer.genres(df_genres=df_genres)
            db_writer.styles(df_styles=df_styles)
            db_writer.credits(df_credits=df_credits)
            db_writer.tracks(df_tracks=df_tracks)
            db_writer.track_artist(df_artists=df_track_artists)
            db_writer.videos(df_videos=df_videos)
            self.__artists.process()
        db_writer.stats(df_stats=df_stats)

    def release(self) -> pd.DataFrame:
        df_release = pd.DataFrame([self.d_release.data])
        cols_release = ['id', 'master_id', 'title', 'thumb', 'cover_image', 'year', 'uri', 'country', 'released']
        cols_df = df_release.columns.values.tolist()
        cols = list(set(cols_release) & set(cols_df))
        df_release = df_release[cols]
        df_release = df_release.rename(columns={'id': 'id_release', 'master_id': 'id_master', 'cover_image': 'url_cover',\
            'thumb': 'url_thumbnail', 'uri': 'url_release', 'released': 'date_released'})
        return df_release

    def release_artists(self) -> pd.DataFrame:
        lst_artists = []
        for artist in self.d_release.artists:
            lst_artists.append({'id_artist': artist.id, 'id_release': self.d_release.id})
        df_artists = pd.DataFrame(lst_artists)
        return df_artists

    def labels(self) -> pd.DataFrame:
        lst_labels = []
        for label in self.d_release.labels:
            df_label = pd.DataFrame([label.data])
            lst_labels.append(df_label)
        df_labels = pd.DataFrame()
        if len(lst_labels) > 0:
            df_labels = pd.concat(lst_labels, axis=0, ignore_index=True)
            df_labels['id_release'] = self.d_release.id
            df_labels = df_labels[['id_release', 'id', 'name', 'catno', 'thumbnail_url']]
            df_labels = df_label.rename(columns={'id': 'id_label', 'name': 'name_label', 'thumbnail_url': 'url_thumbnail'})
        return df_labels

    def formats(self) -> pd.DataFrame:
        lst_formats = []
        for format in self.d_release.formats:
            df_format = pd.DataFrame([format])
            lst_formats.append(df_format)
        df_formats = pd.DataFrame()
        if len(lst_formats) > 0:
            df_formats = pd.concat(lst_formats, axis=0, ignore_index=True)
            df_formats = df_formats[['name', 'qty']]
            df_formats['id_release'] = self.d_release.id
            df_formats = df_formats.rename(columns={'name': 'name_format', 'qty': 'qty_format'})
        return df_formats

    def credits(self) -> pd.DataFrame:
        lst_artist = []
        df_artists = pd.DataFrame()
        for artist in self.d_release.credits:
            df_artist = pd.DataFrame([artist.data])
            lst_artist.append(df_artist)
        if len(lst_artist) > 0:
            df_artists = pd.concat(lst_artist, axis=0, ignore_index=True)
            df_artists = df_artists[['name', 'role', 'id', 'resource_url', 'thumbnail_url']]  
            df_artists = df_artists.rename(columns={'name': 'name_artist', 'id': 'id_artist', 'resource_url': 'api_artist',\
                'thumbnail_url': 'url_thumbnail'})      
            df_artists['id_release'] = self.d_release.id 
        return df_artists

    def stats(self) -> pd.DataFrame:
        df_stats = pd.DataFrame([self.d_release.data])
        df_stats = df_stats[['id', 'num_for_sale', 'lowest_price']]
        df_stats['time_value_retrieved'] = dt.datetime.now()
        df_stats = df_stats.rename(columns={'id': 'id_release', 'num_for_sale': 'qty_for_sale', 'lowest_price': 'amt_price_lowest'})        
        dict_community = {key: self.d_release.data['community'][key] for key in ['have', 'want']}
        df_community = pd.DataFrame([dict_community])
        df_community = df_community.rename(columns={'have': 'qty_has', 'want': 'qty_want'}) 
        df_stats = pd.concat([df_stats, df_community], axis=1, join='inner')
        return df_stats


class Collection():
    def __init__(self, db_file: str) -> None:
        self.db_writer = _db_writer.Collection(db_file=db_file)

    def value(self, user: discogs_client.User) -> None:
        df_value = pd.DataFrame([user.collection_value.data])
        df_value['qty_items'] = user.num_collection
        df_value['time_value_retrieved'] = dt.datetime.now()
        self.db_writer.value(df_value=df_value)


class CollectionItem():
    def __init__(self, item: discogs_client.CollectionItemInstance, db_file: str) -> None:
        self.__item = item
        self.__release = Release(release=item.release, db_file=db_file)
        self.db_file = db_file

    def process(self) -> None:
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        df_item = self.__collection_item()
        self.__release.process()
        db_writer.items(df_items=df_item)

    def __collection_item(self) -> pd.DataFrame:
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
        return df_item


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
    