import datetime as dt

import igraph as igraph
import polars as pl
from tqdm import tqdm
from discogs_client.models import Artist, CollectionItemInstance



class ArtistsDerive:
    """A class that processes artist related data"""

    def __init__(self, artists: Artist, db_file: str) -> None:
        self.__d_artists = artists
        self.db_file = db_file
        self.process_masters = True

    def process(self) -> None:
        db_writer = _db_writer.ArtistsWriter(file_db=self.db_file)
        for artist in tqdm(
            self.__d_artists, total=len(self.__d_artists), desc="Artists"
        ):
            exists = db_writer.in_db(id_artist=artist.id)
            if not exists:
                df_artists = self.artist(artist=artist)
                if self.process_masters:
                    df_masters = self.masters(artist=artist)
                df_images = self.images(artist=artist)
                df_aliases = self.aliases(artist=artist)
                df_groups = self.groups(artist=artist)
                df_members = self.members(artist=artist)
                df_urls = self.urls(artist=artist)
                db_writer.artists(df_artists=df_artists)
                if self.process_masters:
                    db_writer.masters(df_masters=df_masters)
                db_writer.members(df_members=df_members)
                db_writer.groups(df_groups=df_groups)
                db_writer.aliases(df_aliases=df_aliases)
                db_writer.images(df_images=df_images)
                db_writer.urls(df_urls=df_urls)

    def process_masters(self) -> None:
        db_writer = _db_writer.ArtistsWriter(file_db=self.db_file)
        for artist in tqdm(
            self.__d_artists, total=len(self.__d_artists), desc="Artists"
        ):
            df_masters = self.masters(artist=artist)
            db_writer.masters(df_masters=df_masters)

    def artist(self, artist: Artist) -> pl.DataFrame:
        df_artist = pl.DataFrame()
        try:
            dict_artist = {"id_artist": artist.id, "name_artist": artist.name}
            df_artist = pl.DataFrame([dict_artist])
        except:
            pass
        return df_artist

    def masters(self, artist: Artist) -> pl.DataFrame:
        masters = []
        df_masters = pl.DataFrame()
        try:
            qty_pages = artist.releases.pages
            for page_no in tqdm(
                range(1, qty_pages),
                total=qty_pages - 1,
                desc=artist.name + " - Masters",
            ):
                page = artist.releases.page(page_no)
                masters = masters + [master.data for master in page]
            df_masters = pl.DataFrame(masters)
            if not df_masters.empty:
                df_masters["id_artist"] = artist.id
                df_masters = df_masters[
                    [
                        "id_artist",
                        "id",
                        "title",
                        "type",
                        "main_release",
                        "artist",
                        "role",
                        "year",
                        "thumb",
                    ]
                ]
                df_masters = df_masters.rename(
                    columns={
                        "id": "id_master",
                        "main_release": "id_main_release",
                        "artist": "name_artist",
                        "thumb": "url_thumb",
                    }
                )
        except:
            pass
        return df_masters

    def images(self, artist: Artist) -> pl.DataFrame:
        try:
            images = []
            df_images = pl.DataFrame()
            for image in artist.images:
                df_image = pl.DataFrame([image])
                images.append(df_image)
            if len(images) > 0:
                df_images = pl.concat(images, axis=0, ignore_index=True)
                df_images["id_artist"] = artist.id
                df_images = df_images[
                    ["id_artist", "type", "uri", "uri150", "width", "height"]
                ]
                df_images = df_images.rename(
                    columns={
                        "uri": "url_image",
                        "uri150": "url_image_150",
                        "width": "width_image",
                        "height": "height_image",
                    }
                )
        except:
            return df_images
        return df_images

    def groups(self, artist: Artist) -> pl.DataFrame:
        groups = []
        df_groups = pl.DataFrame()
        try:
            for group in artist.groups:
                df_group = pl.DataFrame([group.data])
                groups.append(df_group)
            if len(groups) > 0:
                df_groups = pl.concat(groups, axis=0, ignore_index=True)
                df_groups["id_artist"] = artist.id
                df_groups = df_groups.rename(
                    columns={
                        "id": "id_group",
                        "name": "name_group",
                        "resource_url": "api_group",
                        "active": "is_active",
                        "thumbnail_url": "url_thumbnail",
                    }
                )
        except:
            pass
        return df_groups

    def aliases(self, artist: Artist) -> pl.DataFrame:
        aliases = []
        df_aliases = pl.DataFrame()
        try:
            for alias in artist.aliases:
                df_alias = pl.DataFrame([alias.data])
                aliases.append(df_alias)
            if len(aliases) > 0:
                df_aliases = pl.concat(aliases, axis=0, ignore_index=True)
                df_aliases["id_artist"] = artist.id
                df_aliases = df_aliases.rename(
                    columns={
                        "id": "id_alias",
                        "name": "name_alias",
                        "resource_url": "api_alias",
                        "thumbnail_url": "url_thumbnail",
                    }
                )
        except:
            pass
        return df_aliases

    def members(self, artist: Artist) -> pl.DataFrame:
        members = []
        df_members = pl.DataFrame()
        try:
            for member in artist.members:
                df_member = pl.DataFrame([member.data])
                members.append(df_member)
            if len(members) > 0:
                df_members = pl.concat(members, axis=0, ignore_index=True)
                df_members["id_artist"] = artist.id
                df_members = df_members.rename(
                    columns={
                        "id": "id_member",
                        "name": "name_member",
                        "resource_url": "api_member",
                        "active": "is_active",
                        "height": "height_image",
                        "thumbnail_url": "url_thumbnail",
                    }
                )
                self.db_writer.members(df_members=df_members)
        except:
            pass
        return df_members

    def urls(self, artist: Artist) -> pl.DataFrame:
        urls = []
        df_urls = pl.DataFrame()
        try:
            for url in artist.urls:
                df_url = pl.DataFrame([url])
                urls.append(df_url)
            if len(urls) > 0:
                df_urls = pl.concat(urls, axis=0, ignore_index=True)
                df_urls["id_artist"] = artist.id
                df_urls = df_urls.set_axis(["url_artist", "id_artist"], axis=1)
        except:
            pass
        return df_urls


class MasterReleasesDerive:
    """A class that processes a list of release related data"""

    def __init__(self, df_masters: pl.DataFrame, db_file: str) -> None:
        self.__df_masters = df_masters
        self.__db_file = db_file

    def process(self) -> None:
        for d_master in tqdm(self.__lst_masters, total=len(self.__lst_masters)):
            master = MasterReleaseDerive(release=d_master, db_file=self.__db_file)
            master.process()


class MasterReleaseDerive:
    def __init__(self, release, db_file: str) -> None:
        self.dict_release = release
        self.db_file = db_file

    def process(self) -> None:
        db_writer = _db_writer.MasterWriter(file_db=self.db_file)
        df_stats = self.stats()
        exists = self.db_writer.in_db(id_master=self.dict_release.id)
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

    def master(self) -> pl.DataFrame:
        pass

    def stats(self) -> pl.DataFrame:
        stats = self.dict_release.data["stats"]
        dict_stats = {
            "id_master": self.dict_release.id,
            "qty_wants": stats["community"]["in_wantlist"],
            "qty_has": stats["community"]["in_collection"],
        }
        df_stats = pl.DataFrame([dict_stats])
        return df_stats

    def styles(self) -> pl.DataFrame:
        styles = []
        df_styles = pl.DataFrame()
        if self.dict_release.styles is not None:
            for style in self.dict_release.styles:
                df_style = pl.DataFrame([style])
                styles.append(df_style)
            df_styles = pl.DataFrame()
            if len(styles) > 0:
                df_styles = pl.concat(styles, axis=0, ignore_index=True)
                df_styles["id_release"] = self.dict_release.id
        return df_styles

    def genres(self) -> pl.DataFrame:
        genres = []
        df_genres = pl.DataFrame()
        for genre in self.dict_release.genres:
            df_genre = pl.DataFrame([genre])
            genres.append(df_genre)
        if len(genres) > 0:
            df_genres = pl.concat(genres, axis=0, ignore_index=True)
            df_genres["id_release"] = self.dict_release.id
            df_genres = df_genres.set_axis(["name_genre", "id_release"], axis=1)
        return df_genres

    def tracks(self) -> pl.DataFrame:
        tracks = []
        df_tracks = pl.DataFrame()
        for track in self.dict_release.tracklist:
            dict_track = track.data
            tracks.append(pl.DataFrame([dict_track]))
        if len(tracks) > 0:
            df_tracks = pl.concat(tracks, axis=0, ignore_index=True)
            df_tracks = df_tracks[["position", "title", "duration"]]
            df_tracks["id_release"] = self.dict_release.id
        return df_tracks

    def track_artists(self) -> pl.DataFrame:
        artists = []
        df_artists = pl.DataFrame()
        for track in self.dict_release.tracklist:
            if "extraartists" in track.data:
                dict_artist = track.data["extraartists"]
                df_artist = pl.DataFrame(dict_artist)
                df_artist["position"] = track.data["position"]
                artists.append(df_artist)
        if len(artists) > 0:
            df_artists = pl.concat(artists, axis=0, ignore_index=True)
            df_artists = df_artists[["name", "role", "id", "resource_url", "position"]]
            df_artists = df_artists.rename(
                columns={
                    "name": "name_artist",
                    "id": "id_artist",
                    "resource_url": "api_artist",
                }
            )
            df_artists["id_release"] = self.dict_release.id
        return df_artists

    def videos(self) -> pl.DataFrame:
        videos = []
        df_videos = pl.DataFrame()
        for video in self.dict_release.videos:
            dict_video = video.data
            videos.append(pl.DataFrame([dict_video]))
        if len(videos) > 0:
            df_videos = pl.concat(videos, axis=0, ignore_index=True)
            df_videos = df_videos[["uri", "title", "duration"]]
            df_videos = df_videos.rename(columns={"uri": "url_video"})
            df_videos["id_release"] = self.dict_release.id
        return df_videos


class ReleaseDerive(MasterReleaseDerive):
    """A class that processes release related data"""

    def __init__(self, release, db_file: str) -> None:
        super().__init__(release, db_file)
        self.__artists = ArtistsDerive(artists=release.artists, db_file=db_file)

    def process(self) -> None:
        db_writer = _db_writer.ReleaseWriter(file_db=self.db_file)
        # df_stats = self.stats()
        exists = db_writer.in_db(id_release=self.dict_release.id)
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
        # db_writer.stats(df_stats=df_stats)

    def release(self) -> pl.DataFrame:
        df_release = pl.DataFrame([self.dict_release.data])
        cols_release = [
            "id",
            "master_id",
            "title",
            "thumb",
            "cover_image",
            "year",
            "uri",
            "country",
            "released",
        ]
        cols_df = df_release.columns.values.tolist()
        cols = list(set(cols_release) & set(cols_df))
        df_release = df_release[cols]
        df_release = df_release.rename(
            columns={
                "id": "id_release",
                "master_id": "id_master",
                "cover_image": "url_cover",
                "thumb": "url_thumbnail",
                "uri": "url_release",
                "released": "date_released",
            }
        )
        return df_release

    def release_artists(self) -> pl.DataFrame:
        artists = []
        for artist in self.dict_release.artists:
            artists.append({"id_artist": artist.id, "id_release": self.dict_release.id})
        df_artists = pl.DataFrame(artists)
        return df_artists

    def labels(self) -> pl.DataFrame:
        labels = []
        for label in self.dict_release.labels:
            df_label = pl.DataFrame([label.data])
            labels.append(df_label)
        df_labels = pl.DataFrame()
        if len(labels) > 0:
            df_labels = pl.concat(labels, axis=0, ignore_index=True)
            df_labels["id_release"] = self.dict_release.id
            df_labels = df_labels[
                ["id_release", "id", "name", "catno"]
            ]
            df_labels = df_label.rename(
                columns={
                    "id": "id_label",
                    "name": "name_label",
                }
            )
        return df_labels

    def formats(self) -> pl.DataFrame:
        formats = []
        for format in self.dict_release.formats:
            df_format = pl.DataFrame([format])
            formats.append(df_format)
        df_formats = pl.DataFrame()
        if len(formats) > 0:
            df_formats = pl.concat(formats, axis=0, ignore_index=True)
            df_formats = df_formats[["name", "qty"]]
            df_formats["id_release"] = self.dict_release.id
            df_formats = df_formats.rename(
                columns={"name": "name_format", "qty": "qty_format"}
            )
        return df_formats

    def credits(self) -> pl.DataFrame:
        artists = []
        df_artists = pl.DataFrame()
        for artist in self.dict_release.credits:
            df_artist = pl.DataFrame([artist.data])
            artists.append(df_artist)
        if len(artists) > 0:
            df_artists = pl.concat(artists, axis=0, ignore_index=True)
            df_artists = df_artists[["name", "role", "id", "resource_url"]]
            df_artists = df_artists.rename(
                columns={
                    "name": "name_artist",
                    "id": "id_artist",
                    "resource_url": "api_artist",
                }
            )
            df_artists["id_release"] = self.dict_release.id
        return df_artists

    def stats(self) -> pl.DataFrame:
        marketplace = self.dict_release.marketplace_stats
        dict_marketplace = marketplace.data
        community = self.dict_release.community
        dict_community = community.data
        dict_stats = {
            "id_release": dict_marketplace["id"],
            "time_value_retrieved": dt.datetime.now(),
            "qty_for_sale": marketplace.num_for_sale,
            "amt_price_lowest": dict_marketplace["lowest_price"]["value"]
            if dict_marketplace["lowest_price"] is not None
            else None,
            "qty_has": dict_community["have"],
            "qty_want": dict_community["want"],
            "avg_rating": dict_community["rating"]["average"],
            "qty_ratings": dict_community["rating"]["count"],
        }
        df_stats = pl.DataFrame([dict_stats])
        return df_stats


class CollectionDerive:
    def __init__(self, db_file: str) -> None:
        self.db_writer = _db_writer.CollectionWriter(file_db=db_file)


class CollectionItemDerive:
    def __init__(self, item: CollectionItemInstance, db_file: str) -> None:
        self.__item = item
        self.__release = ReleaseDerive(release=item.release, db_file=db_file)
        self.db_file = db_file

    def process(self) -> None:
        db_writer = _db_writer.CollectionWriter(file_db=self.db_file)
        df_item = self.__collection_item()
        self.__release.process()
        db_writer.items(df_items=df_item)

    def __collection_item(self) -> pl.DataFrame:
        dict_item = {
            "id_release": self.__item.data["id"],
            "date_added": self.__item.data["date_added"],
            "id_instance": self.__item.data["instance_id"],
            "title": self.__item.data["basic_information"]["title"],
            "id_master": self.__item.data["basic_information"]["master_id"],
            "api_master": self.__item.data["basic_information"]["master_url"],
            "api_release": self.__item.data["basic_information"]["resource_url"],
            "url_thumbnail": self.__item.data["basic_information"]["thumb"],
            "url_cover": self.__item.data["basic_information"]["cover_image"],
            "year_released": self.__item.data["basic_information"]["year"],
            "rating": self.__item.data["rating"],
        }
        df_item = pl.DataFrame(dict_item, index=[0])
        return df_item


class ArtistNetworkDerive:
    def __init__(self, df_vertices: pl.DataFrame, df_edges: pl.DataFrame) -> None:
        self.df_vertices = df_vertices
        self.df_edges = df_edges

    def cluster_betweenness(self) -> pl.DataFrame:
        df_hierarchy = pl.DataFrame()
        return df_hierarchy

    def centrality(self) -> pl.DataFrame:
        df_centrality = pl.DataFrame()
        return df_centrality
