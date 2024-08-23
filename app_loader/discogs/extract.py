import datetime as dt
import sqlite3

import pandas as pd
import igraph as ig
from tqdm import tqdm
from discogs_client import Client

from discogs.db_utils import DBStorage
import discogs.derive as _derive
import discogs.db_writer as _db_writer
import discogs.db_reader as _db_reader


class Extractor(DBStorage):
    """A class for extracting, processing and storing a user's collection data from Discogs"""

    def __init__(self, client_discogs: Client, db_file: str) -> None:
        super().__init__(db_file)
        self.client_discogs = client_discogs
        self.user = client_discogs.identity()

    def start(self) -> None:
        """Starts user's collection processing"""
        self.collection_value()
        self.collection_items()
        # self.artist_set_attributes()
        # self.artists_from_collection()
        # self.extract_artist_edges()
        # self.create_clusters()
        # self.similar_dissimilar()

    def collection_value(self) -> None:
        """ Collection value"""
        db_writer = _db_writer.Collection(db_file=self.db_file)
        collection_value = self.user.collection_value
        dict_stats = {
            "time_value_retrieved": dt.datetime.now(),
            "qty_collection_items": self.user.num_collection,
            "amt_maximum":collection_value.maximum,
            "amt_median": collection_value.median,
            "amt_minumum": collection_value.minimum
        }
        db_writer.value(pd.DataFrame([dict_stats]))


    def collection_items(self) -> None:
        """Process the user's collection items"""
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        qty_items = self.user.collection_folders[0].count
        for item in tqdm(
            self.user.collection_folders[0].releases, total=qty_items, desc="Collection items"
        ):
            derive = _derive.CollectionItem(item=item, db_file=self.db_file)
            derive.process()

    def __extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction"""
        df_vertices = self.__artist_vertices()
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
        df_ignore = pd.DataFrame({"id_artist": graph.vs[vtx_to_exclude]["name"]})
        db_writer = _db_writer.Artists(db_file=self.db_file)
        db_writer.ignore_list(df_ignore=df_ignore)

    def artists_from_collection(self) -> None:
        """Process artist information derived from groups and memberships"""
        db_reader = _db_reader.Collection(db_file=self.db_file)
        db_writer = _db_writer.Collection(db_file=self.db_file)
        self.__extract_artist_to_ignore()
        qty_artists_not_added = db_reader.qty_artists_not_added()
        while qty_artists_not_added > 0:
            df_write_attempts = db_reader.artists_write_attempts()
            df_artists_new = db_reader.artists_not_added()
            artists = []
            for index, row in df_artists_new.iterrows():
                artists.append(self.client_discogs.artist(id=row["id_artist"]))
                df_write_attempts = pd.concat(
                    [
                        df_write_attempts,
                        pd.DataFrame.from_records(
                            [{"id_artist": row["id_artist"], "qty_attempts": 1}]
                        ),
                    ]
                )
                # df_write_attempts = df_write_attempts.append({'id_artist': row['id_artist'], 'qty_attempts': 1}, ignore_index=True)
            derive = _derive.Artists(artists=artists, db_file=self.db_file)
            derive.process_masters = False
            derive.process()
            self.__extract_artist_to_ignore()
            df_write_attempts = (
                df_write_attempts.groupby(["id_artist"])["qty_attempts"]
                .sum()
                .reset_index()
            )
            db_writer.artist_write_attempts(df_write_attempts=df_write_attempts)
            qty_artists_not_added = db_reader.qty_artists_not_added()

    def masters_from_artists(self) -> None:
        """Process master release information from artists"""
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.artists()
        artists = []
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
            artists.append(self.client_discogs.artist(id=row["id_artist"]))
        derive = _derive.Artists(artists=artists)
        derive.process_masters()

    def similar_dissimilar(self) -> None:
        self.execute_sql_file(file_name="loading/sql/spinder.sql")
