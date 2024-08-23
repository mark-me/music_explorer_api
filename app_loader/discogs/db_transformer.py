import igraph as ig
import pandas as pd

from discogs.db_utils import DBStorage
import discogs.db_writer as _db_writer


class DBTransform(DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)
        pass

    def artist_is_groups(self) -> None:
        self.column_add(name_table="artist", name_column="is_group", type_data="INT")
        sql_statement = "CREATE INDEX IF NOT EXISTS idx_artist_members_id_artist ON artist_members (id_artist);"
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist);
        """
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET is_group = 0
        WHERE is_group IS NULL;
        """
        self.execute_sql(sql=sql_statement)

    def artist_thumbnails(self) -> None:
        self.column_add(
            name_table="artist", name_column="url_thumbnail", type_data="VARCHAR"
        )
        self.drop_existing_table(name_table="thumbnails")
        sql_statement = """
        CREATE TABLE thumbnails AS
        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases
        UNION
        SELECT id_member, url_thumbnail FROM artist_members
        UNION
        SELECT id_group, url_thumbnail FROM artist_groups;
        """
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET url_thumbnail = ( SELECT url_thumbnail WHERE id_artist = artist.id_artist );
        """
        self.execute_sql(sql=sql_statement)
        # self.drop_existing_table(name_table="thumbnails")

    def artist_collection_items(self) -> None:
        self.column_add(
            name_table="artist", name_column="qty_collection_items", type_data="INT"
        )

        self.drop_existing_table(name_table="qty_collection_items")
        sql_statement = """
        CREATE TABLE qty_collection_items AS
            SELECT release_artists.id_artist AS id_artist, COUNT(*) AS qty_items
            FROM collection_items
            INNER JOIN release_artists
                ON release_artists.id_release = collection_items.id_release
            GROUP BY release_artists.id_artist;
        """

        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET qty_collection_items = (SELECT qty_items
                                    FROM qty_collection_items
                                    WHERE  qty_collection_items.id_artist = artist.id_artist);
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="qty_collection_items")

    def load_roles(self) -> None:
        has_table = self.table_exists(name_table="role")
        if not has_table:
            sql_statement = """
            CREATE TABLE role AS
                SELECT DISTINCT
                    role,
                    0 AS as_edge
                FROM release_credits
            """
            self.execute_sql(sql=sql_statement)
        list_value_part = [
            "piano",
            "vocal",
            "perform",
            "bass",
            "viol",
            "drum",
            "keyboard",
            "guitar",
            "sax",
            "music",
            "written",
            "arrange",
            "lyric",
            "word",
            "compose",
            "song",
            "accordion",
            "chamberlin",
            "clarinet",
            "banjo",
            "band",
            "bongo",
            "bell",
            "bouzouki",
            "brass",
            "cello",
            "cavaquinho",
            "celest",
            "choir",
            "chorus",
            "handclap",
            "conduct",
            "conga",
            "percussion",
            "trumpet",
            "cornet",
            "djembe",
            "dobro",
            "organ",
            "electron",
            "horn",
            "fiddle",
            "flute",
            "recorder",
            "glocken",
            "gong",
            "guest",
            "vibra",
            "harmonium",
            "harmonica",
            "harp",
            "beatbox",
            "leader",
            "loop",
            "MC",
            "mellotron",
            "melod",
            "mixed",
            "oboe",
            "orchestra",
            "recorded",
            "remix",
            "saw",
            "score",
            "sitar",
            "strings",
            "synth",
            "tabla",
            "tambourine",
            "theremin",
            "timbales",
            "timpani",
            "whistle",
            "triangle",
            "trombone",
            "tuba",
            "vocoder",
            "voice",
            "phone",
            "woodwind",
        ]
        last_value = list_value_part.pop(-1)
        sql_start = "UPDATE role SET as_edge = 1 WHERE "
        sql_statement = (
            sql_start
            + "".join(
                f"""role LIKE "%{value_part}%" OR """ for value_part in list_value_part
            )
            + f"""role LIKE "%{last_value}%" """
        )
        self.execute_sql(sql=sql_statement)

    def artist_relationships(self) -> None:
        self.drop_existing_table(name_table="artist_relations")
        sql_statement = """
        CREATE TABLE artist_relations AS
            SELECT DISTINCT id_artist_from,
                            id_artist_to,
                            relation_type
            FROM (  SELECT id_member AS id_artist_from,
                        id_artist as id_artist_to,
                        'group_member' as relation_type
                    FROM artist_members
                UNION
                    SELECT id_artist as id_artist_from,
                        id_group AS id_artist_to,
                        'group_member' as relation_type
                    FROM artist_groups
                UNION
                    SELECT a.id_alias,
                        a.id_artist,
                        'artist_alias'
                    FROM artist_aliases a
                    LEFT JOIN artist_aliases b
                        ON a.id_artist = b.id_alias AND
                            a.id_alias = b.id_artist
                    WHERE a.id_artist > b.id_artist OR
                        b.id_artist IS NULL
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'co_appearance'
                    FROM release_artists a
                    INNER JOIN release_artists b
                        ON b.id_release = a.id_release
                    WHERE a.id_artist != b.id_artist
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'release_role'
                    FROM release_artists a
                    INNER JOIN release_credits b
                        ON b.id_release = a.id_release
                    INNER JOIN role c
                        ON c.role = b.role
                    WHERE a.id_artist = b.id_artist AND
                        as_edge = 1
                )
            INNER JOIN artist a
                ON a.id_artist = id_artist_from
            INNER JOIN artist b
                ON b.id_artist = id_artist_to
            GROUP BY id_artist_from,
                id_artist_to,
                relation_type;
        """
        self.execute_sql(sql=sql_statement)

    def artist_vertices(self) -> None:
        """Retrieve artists in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_vertex")
        sql_statement = """
            CREATE TABLE artist_vertex AS
                SELECT id_artist, MAX(in_collection) AS in_collection
                FROM (  SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist
                            UNION
                        SELECT id_alias, 0 as in_collection from artist_aliases
                            UNION
                        SELECT id_member, 0 FROM artist_members
                            UNION
                        SELECT id_group, 0 FROM artist_groups
                            UNION
                        SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')
                            UNION
                        SELECT release_artists.id_artist, MAX(IIF(date_added IS NULL, 0, 1))
                        FROM release_artists
                        INNER JOIN release
                            ON release.id_release = release_artists.id_release
                        LEFT JOIN collection_items
                            ON collection_items.id_release = release.id_release
                        GROUP BY release_artists.id_artist )
                GROUP BY id_artist"""
        self.execute_sql(sql=sql_statement)

    def artist_edges(self) -> None:
        """Retrieve artist cooperations in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_edge")
        sql_statement = """
            CREATE TABLE artist_edge AS
                SELECT DISTINCT id_artist_from, id_artist_to, relation_type
                FROM (  SELECT id_member AS id_artist_from, id_artist as id_artist_to, 'group_member' as relation_type
                        FROM artist_members
                    UNION
                        SELECT id_artist, id_group, 'group_member' FROM artist_groups
                    UNION
                        SELECT a.id_alias, a.id_artist, 'artist_alias'
                        FROM artist_aliases a
                        LEFT JOIN artist_aliases b
                            ON a.id_artist = b.id_alias AND
                                a.id_alias = b.id_artist
                        WHERE a.id_artist > b.id_artist OR b.id_artist IS NULL
                    UNION
                        SELECT a.id_artist, b.id_artist, 'co_appearance'
                        FROM release_artists a
                        INNER JOIN release_artists b
                            ON b.id_release = a.id_release
                        WHERE a.id_artist != b.id_artist )
                GROUP BY id_artist_from, id_artist_to, relation_type;"""
        self.execute_sql(sql=sql_statement)

    def __extract_artist_to_ignore(self) -> None:
        """Define which artists to exclude from discogs extraction"""
        df_vertices = self.read_table(name_table="artist_vertex")
        df_edges = self.read_table(name_table="artist_edge")
        graph = ig.Graph.DataFrame(
            edges=df_edges,
            directed=False,
            # vertices=df_vertices,
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

    def __get_artist_graph(self) -> None:
        lst_edges = self.read_sql(
            sql="SELECT * FROM artist_relations WHERE id_artist_from != id_artist_to"
        ).to_dict(orient="records")
        lst_vertices = self.read_sql(
            sql="SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist"
        ).to_dict(orient="records")
        graph = ig.Graph.DictList(
            vertices=lst_vertices,
            edges=lst_edges,
            vertex_name_attr="id_artist",
            edge_foreign_keys=("id_artist_from", "id_artist_to"),
        )
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
                            "id_artist": graph.vs["id_artist"],
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
                        "id_artist": component.vs["id_artist"],
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
        # self.execute_sql_file(
        #     file_name="loading/sql/extract_community_dendrogram.sql"
        # )  # Create a summary of the clustering hierarchy

    def start(self) -> None:
        self.artist_is_groups()
        self.artist_thumbnails()
        self.artist_collection_items()
        self.load_roles()
        self.artist_relationships()
        self.artist_vertices()
        self.artist_edges()
        self.create_clusters()
