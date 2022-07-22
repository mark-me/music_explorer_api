import sys
import datetime as dt
import time
import yaml
import sqlite3
import numpy as np
import pandas as pd
import igraph as ig
import networkx as nx
from pyvis.network import Network
from tqdm import tqdm

import derive as _derive
import db_utils as _db_utils
import db_writer as _db_writer
import db_reader as _db_reader

with open(r'config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
db_file = config['db_file']

class Database(_db_utils.DBStorage):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file)

    def extract_artist_edges(self) -> None:
        self.execute_sql_file(file_name="loading/sql/extract_artist_relations.sql")

    def __get_artist_graph(self) -> ig.Graph:
        artists = _db_reader.Artists(db_file=self.db_file)
        df_edges = artists.edges()
        df_vertices = artists.vertices()[['id_artist', 'name_artist', 'in_collection', 'qty_collection_items']]
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

    def create_clusters(self) -> None:
        graph_all = self.__get_artist_graph()
        # Cluster all components
        lst_components = graph_all.decompose()  # Decompose graph
        lst_dendrogram = []
        for component in lst_components:
            if sum(component.vs['in_collection']) <= 2:
                qty_vertices = len(component.vs)
                df_dendrogram = pd.DataFrame({'id_artist': component.vs['name'],
                                            'name_artist': component.vs['name_artist'],
                                            'in_collection': component.vs['in_collection'],
                                            'qty_collection_items': component.vs['qty_collection_items'],
                                            'id_hierarchy': [0] * qty_vertices,
                                            'id_community_from': [0] * qty_vertices,
                                            'id_community': [1] * qty_vertices,
                                            'eigenvalue': component.eigenvector_centrality(directed=False)})
            else:
                df_dendrogram = self.__cluster_component(component)
            lst_dendrogram.append(df_dendrogram)
        # Making all community id's unique and add root to connect to components
        community_max = 0
        for i in range(len(lst_dendrogram)):
            # Make communities unique across the dendrograms
            df_component = lst_dendrogram[i]
            df_component['id_community'] = df_component['id_community'] + community_max + 1
            if len(set(df_component['id_community_from'])) > 1:
                print("Add extra node as root")
                df_component.loc[:, 'id_community_from'] = df_component.loc[:, 'id_community_from'] + community_max + 1
                df_root = df_component.loc[df_component['id_hierarchy'] == 0].copy()
                df_root['id_community'] = df_root['id_community_from']
                df_root['id_community_from'] = 0
                df_component.loc[:, 'id_hierarchy'] = df_component.loc[:, 'id_hierarchy'] + 1
                df_component = pd.concat([df_component, df_root],axis=0, ignore_index=True)
            community_max = max(df_component['id_community'])
            lst_dendrogram[i] = df_component
        df_hierarchy = pd.concat(lst_dendrogram, axis=0, ignore_index=True)
        db_writer = _db_writer.ArtistNetwork(db_file=self.db_file)
        db_writer.community_hierarchy(df_hierarchy=df_hierarchy)

    def __cluster_component(self, graph_component: ig.Graph) -> pd.DataFrame:
        idx_community_start = 0
        graph_component.vs['id_community_from'] = idx_community_start
        # Queue for processing graphs, keeping track level in community tree and relationships between branches
        lst_processing_queue = [{'graph': graph_component, 'tree_level': 0}]
        # For each sub graph until none in the list
        qty_graphs_queued = len(lst_processing_queue)   # Number of graphs in the community tree
        lst_communities = []                           # Data-frames with data for a processed graph
        while qty_graphs_queued > 0:
            dict_queue = lst_processing_queue.pop(0)
            graph = (dict_queue.get('graph')).simplify()  # Remove self referential and double links
            tree_level = dict_queue.get('tree_level')
            qty_collection_items = sum(graph.vs['in_collection'])
            if qty_collection_items > 2:    # Only determine communities if the number of vertices is higher than x
                cluster_hierarchy = graph.community_fastgreedy()    # communities
                # Setting maximum and minimum of number of clusters
                qty_clusters = 15 if cluster_hierarchy.optimal_count > 15 else cluster_hierarchy.optimal_count
                qty_clusters = qty_clusters if qty_clusters > 2 else 2
                cluster_communities = cluster_hierarchy.as_clustering(n=qty_clusters)   # Determine communities
                community_membership = cluster_communities.membership
                communities = set(community_membership)
                eigenvalue = []
                for community in communities:
                    graph_sub = cluster_communities.subgraph(community)
                    graph_sub.vs['id_community_from'] = [community + (idx_community_start + 1)] * len(graph_sub.vs)
                    lst_processing_queue.append({'graph': graph_sub.copy(), 'tree_level': tree_level + 1})
                    eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(directed=False)  # Calculate eigenvalue per sub_graph
                # Make sure community numbers are unique
                community_membership = [i + (idx_community_start + 1) for i in community_membership]
                idx_community_start = max(community_membership)
                lst_communities.append(pd.DataFrame({'id_artist': graph.vs['name'],
                                                    'name_artist': graph.vs['name_artist'],
                                                    'in_collection': graph.vs['in_collection'],
                                                    'qty_collection_items': graph.vs['qty_collection_items'],
                                                    'id_hierarchy': [tree_level] * len(graph.vs),
                                                    'id_community_from': graph.vs['id_community_from'],
                                                    'id_community': community_membership,
                                                    'eigenvalue': eigenvalue}))
            qty_graphs_queued = len(lst_processing_queue)
        return(pd.concat(lst_communities, axis=0, ignore_index=True))

    def extract_community_dendrogram(self) -> None:
        self.execute_sql_file(file_name="loading/sql/extract_community_dendrogram.sql")


def plot_interactive(db_file: str) -> None:
    artists = _db_reader.Artists(db_file=db_file)
    df_community_edges = artists.community_hierarchy_edges()
    df_community_vertices = artists.community_hierarchy_vertices()
    max_qty = np.nanmax(df_community_vertices['qty_artists_collection'])
    min_qty = np.nanmin(df_community_vertices['qty_artists_collection'])
    df_community_vertices['size'] = [round((i - min_qty)/(max_qty - min_qty)* 100 + 10) for i in df_community_vertices['qty_artists_collection']]
    df_community_vertices['label'] = df_community_vertices['id_community'].map(str)
    df_community_vertices['color'] = ['#d45087' if i >= 1 else '#2f4b7c' for i in df_community_vertices['qty_artists_collection']]
    df_community_vertices.label_community_collection.fillna(df_community_vertices.label_community, inplace=True)
    df_community_vertices['title'] = '# Artists: ' + df_community_vertices['qty_artists'].map(str) + '\n' +\
        '# Artists in collection: ' + df_community_vertices['qty_artists_collection'].map(str) + '\n' +\
        df_community_vertices['label_community_collection'].map(str)
    df_community_edges['color'] = ['#d45087' if i > 0 else '#2f4b7c' for i in df_community_edges['to_collection_artists']]
    network = nx.from_pandas_edgelist(df_community_edges, source = 'id_from', target = 'id_to')
    node_attr = df_community_vertices.set_index('id_community').to_dict('index')
    nx.set_node_attributes(network, node_attr)
    visnet = Network(height = "100%", width = "100%", bgcolor='#222222', font_color='white')
    visnet.from_nx(network)
    visnet.show('graph_community.html')


# Create overall graph
db = Database(db_file=db_file)
# db.extract_artist_edges()
# db.create_clusters()
db.extract_community_dendrogram()
plot_interactive(db_file=db_file)
