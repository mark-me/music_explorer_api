import datetime as dt

import igraph as ig
import polars as pl
from discogs_client import Client
from discogs_client.models import Artist, CollectionItemInstance
from tqdm import tqdm

from app_loader.db_operations import (
    ArtistsReader,
    ArtistsWriter,
    CollectionReader,
    CollectionWriter,
)

from . import derive as _derive


class DiscogsExtractor:
    """A class for extracting, processing and storing a user's collection data from Discogs"""

    def __init__(self, discogs_client: Client, file_db: str) -> None:
        self._file_db = file_db
        self._discogs_client = discogs_client
        self._user = discogs_client.identity()


class DiscogsCollection(DiscogsExtractor):
    def __init__(self, discogs_client: Client, file_db: str):
        super().__init__(discogs_client, file_db)
        self.CollectionWriter = CollectionWriter(file_db=file_db)

    def collection_value(self) -> None:
        """Collection value"""
        collection_value = self.user.collection_value
        df_stats = pl.DataFrame(
            [
                {
                    "time_value_retrieved": dt.datetime.now(),
                    "qty_collection_items": self.user.num_collection,
                    "amt_maximum": collection_value.maximum,
                    "amt_median": collection_value.median,
                    "amt_minumum": collection_value.minimum,
                }
            ]
        )
        self.CollectionWriter.value(df_stats)

    def collection_items(self) -> None:
        """Process the user's collection items"""
        self.CollectionWriter.drop_tables()
        qty_items = self.user.collection_folders[0].count
        for item in tqdm(
            self._user.collection_folders[0].releases,
            total=qty_items,
            desc="Collection items",
        ):
            self._collection_item(collection_item=item)

    def _collection_item(self, collection_item: CollectionItemInstance) -> pl.DataFrame:
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


class DiscogsArtists(DiscogsExtractor):
    def __init__(self, discogs_client: Client, file_db: str):
        super().__init__(discogs_client, file_db)
        self.artists_reader = ArtistsReader(file_db=file_db)
        self.artists_writer = ArtistsWriter(file_db=file_db)
        self.collection_reader = CollectionReader(file_db=file_db)
        self.collection_writer = CollectionWriter(file_db=file_db)

    def __extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction"""
        df_vertices = self.read_table(name_table="artist_vertex")
        df_edges = self.__artist_edges()
        graph = ig.Graph.DataFrame(
            edges=df_edges,
            directed=False,
            vertices=df_vertices,
        )
        # Select relevant vertices
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_relevant = []
        for vtx in vtx_collection:
            vtx_neighbors = graph.neighborhood(
                vertices=vtx, order=2
            )  # Only query those that have less than 3 steps
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_relevant = list(set(vtx_neighbors + vtx_relevant))
            vtx_connectors = graph.get_shortest_paths(
                vtx, to=vtx_collection
            )  # Vertices that connect artists in the collection
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_relevant = list(set(vtx_connectors + vtx_relevant))
        # Get vertices to ignore
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_relevant))
        df_ignore = pl.DataFrame({"id_artist": graph.vs[vtx_to_exclude]["name"]})
        self.artists_writer.ignore_list(df_ignore=df_ignore)

    def artists_from_collection(self) -> None:
        """Process artist information derived from groups and memberships"""
        self.__extract_artist_to_ignore()
        qty_artists_not_added = self.collection_reader.qty_artists_not_added()
        while qty_artists_not_added > 0:
            df_write_attempts = self.collection_reader.artists_write_attempts()
            df_artists_new = self.collection_reader.artists_not_added()
            artists = []
            for _, row in df_artists_new.iterrows():
                artists.append(self.client_discogs.artist(id=row["id_artist"]))
                df_write_attempts = pl.concat(
                    [
                        df_write_attempts,
                        pl.DataFrame.from_records(
                            [{"id_artist": row["id_artist"], "qty_attempts": 1}]
                        ),
                    ]
                )
                # df_write_attempts = df_write_attempts.append({'id_artist': row['id_artist'], 'qty_attempts': 1}, ignore_index=True)
            derive = _derive.ArtistsDerive(artists=artists, db_file=self.db_file)
            derive.process_masters = False
            derive.process()
            self.__extract_artist_to_ignore()
            df_write_attempts = (
                df_write_attempts.groupby(["id_artist"])["qty_attempts"]
                .sum()
                .reset_index()
            )
            self.collection_writer.artist_write_attempts(
                df_write_attempts=df_write_attempts
            )
            qty_artists_not_added = self.collection_reader.qty_artists_not_added()

    def masters_from_artists(self) -> None:
        """Process master release information from artists"""
        df_artists = self.artists_reader.artists()
        artists = []
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
            artists.append(self.client_discogs.artist(id=row["id_artist"]))
        derive = _derive.ArtistsDerive(artists=artists)
        derive.process_masters()

    def artist(self, id_artist: int) -> None:
        artist = self.client_discogs.artist(id=id_artist)
        derive = _derive.ArtistsDerive(artists=artist, db_file=self.db_file)
        derive.process()

    def similar_dissimilar(self) -> None:
        self.execute_sql_file(file_name="loading/sql/spinder.sql")
