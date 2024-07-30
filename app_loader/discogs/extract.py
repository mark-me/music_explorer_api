import os
import sqlite3
import yaml

import pandas as pd
import igraph as ig
from tqdm import tqdm
import discogs_client

import derive as _derive
from db_utils import DBStorage
import db_writer as _db_writer
import db_reader as _db_reader


class Discogs(DBStorage):
    """A class for extracting, processing and storing a user's collection data from Discogs"""

    def __init__(self, consumer_key: str, consumer_secret: str, db_file: str) -> None:
        super().__init__(db_file)
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.client = self.__set_user_tokens()

    def __set_user_tokens(self) -> discogs_client.Client:
        """Set-up the user's account to use with for the extraction using Discogs API"""
        file_user_token = "/data/user_tokens.yml"
        has_token = False
        if os.path.isfile(file_user_token):
            with open(file_user_token) as file:
                dict_token = yaml.load(file, Loader=yaml.FullLoader)
            has_token = "token" in dict_token and "secret" in dict_token
        if not has_token:
            d = discogs_client.Client(
                user_agent="music_collection_api/1.0",
                consumer_key=self.consumer_key,
                consumer_secret=self.consumer_secret,
            )
            url = d.get_authorize_url()
            print("Visit " + url[2] + " to allow the client to access")
            code_verify = input("Enter the verification code: ")
            access_token = d.get_access_token(code_verify)
            dict_token = {"token": access_token[0], "secret": access_token[1]}
            with open(file_user_token, "w") as file:
                yaml.dump(dict_token, file)
        else:
            d = discogs_client.Client(
                user_agent="music_collection_api/1.0",
                consumer_key=self.consumer_key,
                consumer_secret=self.consumer_secret,
                token=dict_token["token"],
                secret=dict_token["secret"],
            )
        return d

    def start(self) -> None:
        """Starts user's collection processing"""
        self.collection_value()
        self.collection_items()
        self.artist_set_attributes()
        self.artists_from_collection()
        self.extract_artist_edges()
        self.create_clusters()
        self.similar_dissimilar()

    def collection_value(self) -> None:
        """Process the user's collection value statistics"""
        derive = _derive.Collection(db_file=self.db_file)
        derive.value(self.client.identity())

    def collection_items(self) -> None:
        """Process the user's collection items"""
        me = self.client.identity()
        db_writer = _db_writer.Collection(db_file=self.db_file)
        db_writer.drop_tables()
        qty_items = me.collection_folders[0].count
        for item in tqdm(
            me.collection_folders[0].releases, total=qty_items, desc="Collection items"
        ):
            derive = _derive.CollectionItem(item=item, db_file=self.db_file)
            derive.process()

    def __artist_vertices(self) -> pd.DataFrame:
        """Retrieve artists in order to determine where to stop discogs extraction"""
        sql = "SELECT id_artist, MAX(in_collection) AS in_collection\
                FROM (  SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist\
                            UNION\
                        SELECT id_alias, 0 as in_collection from artist_aliases\
                            UNION\
                        SELECT id_member, 0 FROM artist_members\
                            UNION\
                        SELECT id_group, 0 FROM artist_groups\
                            UNION\
                        SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')\
                            UNION\
                        SELECT release_artists.id_artist, MAX(IIF(date_added IS NULL, 0, 1))\
                        FROM release_artists\
                        INNER JOIN release\
                            ON release.id_release = release_artists.id_release\
                        LEFT JOIN collection_items\
                            ON collection_items.id_release = release.id_release\
                        GROUP BY release_artists.id_artist )\
                GROUP BY id_artist"
        db_con = sqlite3.connect(self.db_file)
        df_vertices = pd.read_sql_query(sql=sql, con=db_con)
        db_con.close()
        return df_vertices

    def __artist_edges(self) -> pd.DataFrame:
        """Retrieve artist cooperations in order to determine where to stop discogs extraction"""
        sql = "SELECT DISTINCT id_artist_from, id_artist_to, relation_type\
                FROM (  SELECT id_member AS id_artist_from, id_artist as id_artist_to, 'group_member' as relation_type\
                        FROM artist_members\
                    UNION\
                        SELECT id_artist, id_group, 'group_member' FROM artist_groups\
                    UNION\
                        SELECT a.id_alias, a.id_artist, 'artist_alias'\
                        FROM artist_aliases a\
                        LEFT JOIN artist_aliases b\
                            ON a.id_artist = b.id_alias AND\
                                a.id_alias = b.id_artist\
                        WHERE a.id_artist > b.id_artist OR b.id_artist IS NULL\
                    UNION\
                        SELECT a.id_artist, b.id_artist, 'co_appearance'\
                        FROM release_artists a\
                        INNER JOIN release_artists b\
                            ON b.id_release = a.id_release\
                        WHERE a.id_artist != b.id_artist )\
                GROUP BY id_artist_from, id_artist_to, relation_type;"
        db_con = sqlite3.connect(self.db_file)
        df_edges = pd.read_sql_query(sql=sql, con=db_con)
        db_con.close()
        return df_edges

    def __extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction"""
        graph = ig.Graph.DataFrame(
            edges=self.__artist_edges(),
            directed=False,
            vertices=self.__artist_vertices(),
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
                artists.append(self.client.artist(id=row["id_artist"]))
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

    def artist_set_attributes(self) -> None:
        """Add attributes to the artist table"""
        self.execute_sql_file(file_name="loading/sql/extract_artist_attributes.sql")

    def extract_artist_edges(self) -> None:
        """Extract artist edges from groups, members, aliases and release cooperation"""
        self.execute_sql_file(file_name="loading/sql/extract_artist_relations.sql")

    def __get_artist_graph(self) -> ig.Graph:
        artists = _db_reader.Artists(db_file=self.db_file)
        df_edges = artists.edges()
        df_vertices = artists.vertices()[
            ["id_artist", "name_artist", "in_collection", "qty_collection_items"]
        ]
        graph = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)
        vtx_collection = graph.vs.select(in_collection_eq=1)
        vtx_connectors_all = []
        for vtx in vtx_collection:
            # Only query those that have less than 3 steps
            vtx_neighbors = graph.neighborhood(vertices=vtx, order=2)
            vtx_neighbors = list(set(vtx_neighbors))
            vtx_connectors_all = list(set(vtx_neighbors + vtx_connectors_all))
            # Integrate connectors
            vtx_connectors = graph.get_shortest_paths(vtx, to=vtx_collection)
            vtx_connectors = [x for l in vtx_connectors for x in l]
            vtx_connectors_all = list(set(vtx_connectors + vtx_connectors_all))
        vtx_to_exclude = list(set(graph.vs.indices) - set(vtx_connectors_all))
        graph.delete_vertices(vtx_to_exclude)
        return graph

    def __cluster_component(self, graph_component: ig.Graph) -> pd.DataFrame:
        idx_community_start = 0
        graph_component.vs["id_community_from"] = idx_community_start
        # Queue for processing graphs, keeping track level in community tree and relationships between branches
        lst_processing_queue = [{"graph": graph_component, "tree_level": 0}]
        # For each sub graph until none in the list
        qty_graphs_queued = len(
            lst_processing_queue
        )  # Number of graphs in the community tree
        lst_communities = []  # Data-frames with data for a processed graph
        while qty_graphs_queued > 0:
            dict_queue = lst_processing_queue.pop(0)
            graph = (
                dict_queue.get("graph")
            ).simplify()  # Remove self referential and double links
            tree_level = dict_queue.get("tree_level")
            qty_collection_items = sum(graph.vs["in_collection"])
            if (
                qty_collection_items > 2
            ):  # Only determine communities if the number of vertices is higher than x
                cluster_hierarchy = graph.community_fastgreedy()  # communities
                # Setting maximum and minimum of number of clusters
                qty_clusters = (
                    15
                    if cluster_hierarchy.optimal_count > 15
                    else cluster_hierarchy.optimal_count
                )
                qty_clusters = qty_clusters if qty_clusters > 2 else 2
                cluster_communities = cluster_hierarchy.as_clustering(
                    n=qty_clusters
                )  # Determine communities
                community_membership = cluster_communities.membership
                communities = set(community_membership)
                eigenvalue = []
                for community in communities:
                    graph_sub = cluster_communities.subgraph(community)
                    graph_sub.vs["id_community_from"] = [
                        community + (idx_community_start + 1)
                    ] * len(graph_sub.vs)
                    lst_processing_queue.append(
                        {"graph": graph_sub.copy(), "tree_level": tree_level + 1}
                    )
                    eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(
                        directed=False
                    )  # Calculate eigenvalue per sub_graph
                # Make sure community numbers are unique
                community_membership = [
                    i + (idx_community_start + 1) for i in community_membership
                ]
                idx_community_start = max(community_membership)
                lst_communities.append(
                    pd.DataFrame(
                        {
                            "id_artist": graph.vs["name"],
                            "name_artist": graph.vs["name_artist"],
                            "in_collection": graph.vs["in_collection"],
                            "qty_collection_items": graph.vs["qty_collection_items"],
                            "id_hierarchy": [tree_level] * len(graph.vs),
                            "id_community_from": graph.vs["id_community_from"],
                            "id_community": community_membership,
                            "eigenvalue": eigenvalue,
                        }
                    )
                )
            qty_graphs_queued = len(lst_processing_queue)
        return pd.concat(lst_communities, axis=0, ignore_index=True)

    def create_clusters(self) -> None:
        graph_all = self.__get_artist_graph()
        # Cluster all components
        lst_components = graph_all.decompose()  # Decompose graph
        lst_dendrogram = []
        for component in lst_components:
            if sum(component.vs["in_collection"]) <= 2:
                qty_vertices = len(component.vs)
                df_dendrogram = pd.DataFrame(
                    {
                        "id_artist": component.vs["name"],
                        "name_artist": component.vs["name_artist"],
                        "in_collection": component.vs["in_collection"],
                        "qty_collection_items": component.vs["qty_collection_items"],
                        "id_hierarchy": [0] * qty_vertices,
                        "id_community_from": [0] * qty_vertices,
                        "id_community": [1] * qty_vertices,
                        "eigenvalue": component.eigenvector_centrality(directed=False),
                    }
                )
            else:
                df_dendrogram = self.__cluster_component(component)
            lst_dendrogram.append(df_dendrogram)
        # Making all community id's unique across the dendrograms and add root to connect to components
        community_max = 0
        for i in range(len(lst_dendrogram)):
            df_component = lst_dendrogram[i]
            df_component["id_community"] = (
                df_component["id_community"] + community_max + 1
            )
            # If a component has multiple sub communities add an extra vertex so they will not be added to the root directly
            if len(set(df_component["id_community_from"])) > 1:
                df_component.loc[:, "id_community_from"] = (
                    df_component.loc[:, "id_community_from"] + community_max + 1
                )
                df_root = df_component.loc[df_component["id_hierarchy"] == 0].copy()
                df_root["id_community"] = df_root["id_community_from"]
                df_root["id_community_from"] = 0
                df_component.loc[:, "id_hierarchy"] = (
                    df_component.loc[:, "id_hierarchy"] + 1
                )
                df_component = pd.concat(
                    [df_component, df_root], axis=0, ignore_index=True
                )
            community_max = max(df_component["id_community"])
            lst_dendrogram[i] = df_component
        df_hierarchy = pd.concat(lst_dendrogram, axis=0, ignore_index=True)
        db_writer = _db_writer.ArtistNetwork(db_file=self.db_file)
        db_writer.community_hierarchy(df_hierarchy=df_hierarchy)
        self.execute_sql_file(
            file_name="loading/sql/extract_community_dendrogram.sql"
        )  # Create a summary of the clustering hierarchy

    def masters_from_artists(self) -> None:
        """Process master release information from artists"""
        db_reader = _db_reader.Artists(db_file=self.db_file)
        df_artists = db_reader.artists()
        artists = []
        for index, row in tqdm(df_artists.iterrows(), total=df_artists.shape[0]):
            artists.append(self.client.artist(id=row["id_artist"]))
        derive = _derive.Artists(artists=artists)
        derive.process_masters()

    def similar_dissimilar(self) -> None:
        self.execute_sql_file(file_name="loading/sql/spinder.sql")
