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
import db_writer as _db_writer
import db_reader as _db_reader

with open(r'config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
db_file = config['db_file']

class Database(_db_reader._DBStorage):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file)

    def get_artist_graph(self) -> ig.Graph:
        artists = _db_reader.Artists(db_file=self.db_file)
        df_edges = artists.edges()
        df_vertices = artists.vertices()[['id_artist', 'name_artist', 'in_collection']]
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

    def extract_community_dendrogram(self) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql_file = open("loading/sql/extract_community_dendrogram.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

    def extract_artist_edges(self) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql_file = open("loading/sql/extract_artist_relations.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

    def create_community_dendrogram(self) -> None:
        artists = _db_reader.Artists(db_file=db_file)
        df_edges = artists.edges()
        df_vertices = artists.vertices()[['id_artist', 'name_artist', 'in_collection']]
        graph_all = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)
        lst_graphs = graph_all.decompose()  # Decompose graph

    def cluster_component(self, graph_component: ig.Graph) -> pd.DataFrame:
        # Queue for processing graphs, keeping track level in community tree and relationships between branches
        lst_processing_queue = [{'graph': graph_component, 'tree_level': 0}]
        # For each sub graph until none in the list
        qty_graphs_queued = len(lst_processing_queue)   # Number of graphs in the community tree
        lst_cluster_data = []                           # Data-frames with data for a processed graph
        idx_community_start = 0
        while qty_graphs_queued > 0:
            dict_queue = lst_processing_queue.pop(0)
            graph = (dict_queue.get('graph')).simplify()  # Remove self referential and double links
            tree_level = dict_queue.get('tree_level')
            qty_collection_items = sum(graph.vs['in_collection'])
            qty_vertices = len(graph.vs)
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
                df_cluster_data = pd.DataFrame({'id_artist': graph.vs['name'],
                                                'name_artist': graph.vs['name_artist'],
                                                'in_collection': graph.vs['in_collection'],
                                                'id_hierarchy': [tree_level] * qty_vertices,
                                                'id_community_from': graph.vs['id_community_from'],
                                                'id_community': community_membership,
                                                'eigenvalue': eigenvalue})
                lst_cluster_data.append(df_cluster_data)
            qty_graphs_queued = len(lst_processing_queue)
        df_data = pd.concat(lst_cluster_data, axis=0, ignore_index=True)
        return(df_data)


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
db.extract_artist_edges()
graph_all = db.get_artist_graph()

# Decompose graph
lst_graphs = graph_all.decompose()
# TODO: Remove
print(len(lst_graphs))
idx = 0
for graph in lst_graphs:
    print([str(idx) + " - " + str(len(graph.vs))]) #  + graph.vs['name_artist']
    idx = idx + 1

""" idx_community_start = len(lst_graphs) + 1
idx_component = 0
lst_component_communities =[]
for graph in  lst_graphs:
    graph = graph.copy()
    graph.vs['id_community_from'] = [idx_component] * len(graph.vs)
    if sum(graph.vs['in_collection']) > 1:
        df_communities_component = db.cluster_component(graph_component=graph, 
                                                        idx_community_start=idx_community_start)
    else:
        df_communities_component = pd.DataFrame({'id_artist': graph.vs['name'],
                                                'name_artist': graph.vs['name_artist'],
                                                'in_collection': graph.vs['in_collection'],
                                                'id_hierarchy': [0] * len(graph.vs),
                                                'id_community_from': graph.vs['id_community_from'],
                                                'id_community': [idx_community_start] * len(graph.vs),
                                                'eigenvalue': graph.eigenvector_centrality(directed=False)})
        pass
    lst_component_communities.append(df_communities_component)
    idx_component = idx_component + 1
    idx_community_start = max(df_communities_component['id_community']) + 1
df_communities = pd.concat(lst_component_communities, axis=0, ignore_index=True) """

graph_component = lst_graphs[1].copy()
print(len(graph_component.vs) )
graph_component.vs['id_community_from'] = [0] * len(graph_component.vs) 
df_communities = db.cluster_component(graph_component=graph_component)  # Start point for tree probing
db_writer = _db_writer.ArtistNetwork(db_file=db_file)
db_writer.cluster_hierarchy(df_hierarchy=df_communities)
db.extract_community_dendrogram()
plot_interactive(db_file=db_file)

sys.exit()


""" def cluster_artist_graph(graph: ig.Graph) -> pd.DataFrame:
    # Queue for processing graphs, keeping track level in community tree and relationships between branches
    lst_processing_queue = [{'graph': graph, 'tree_level': 0}]

    # For each sub graph until none in the list
    qty_graphs_queued = len(lst_processing_queue)   # Number of graphs in the community tree
    lst_cluster_data = []                           # Data-frames with data for a processed graph
    community_last = 0
    while qty_graphs_queued > 0:
        dict_queue = lst_processing_queue.pop(0)
        graph = (dict_queue.get('graph')).simplify()  # Remove self referential and double links
        tree_level = dict_queue.get('tree_level')
        
        qty_collection_items = sum(graph.vs['in_collection'])
        qty_vertices = len(graph.vs)
        print("Tree depth: " + str(tree_level) + " - Total graph # vertices: " + str(len(graph.vs))) # TODO: Remove
        # Only cluster if the number of vertices is higher than 15
        if qty_collection_items > 2:
            # Cluster
            tic = time.perf_counter()  # TODO: Remove
            #cluster_hierarchy = graph.community_edge_betweenness(directed=False)
            cluster_hierarchy = graph.community_fastgreedy()
            toc = time.perf_counter()  # TODO: Remove
            print(f"Finished clustering in {toc - tic:0.4f} seconds")  # TODO: Remove
            # Setting maximum and minimum of number of clusters
            qty_clusters = 15 if cluster_hierarchy.optimal_count > 15 else cluster_hierarchy.optimal_count
            qty_clusters = qty_clusters if qty_clusters > 2 else 2
            # Determine clusters
            cluster_communities = cluster_hierarchy.as_clustering(n=qty_clusters)
            community_membership = cluster_communities.membership
            
            qty_vertices_sub = 0  # TODO: Remove
            communities = set(community_membership)
            eigenvalue = []
            for community in communities:
                graph_sub = cluster_communities.subgraph(community)
                graph_sub.vs['id_community_from'] = [community + (community_last + 1)] * len(graph_sub.vs)
                print("Community " + str(community) + " has " + str(len(graph_sub.vs)) + " vertices")  # TODO: Remove
                qty_vertices_sub = qty_vertices_sub + len(graph_sub.vs)  # TODO: Remove
                lst_processing_queue.append({'graph': graph_sub.copy(), 'tree_level': tree_level + 1})
                # Calculate eigenvalue per sub_graph
                eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(directed=False)
            print("Vertices processed: " + str(qty_vertices_sub)) # TODO: Remove
        # else:
        #     community_membership = [0] * qty_vertices
        #     eigenvalue = [1] * qty_vertices
            
        # Make sure community numbers are unique    
            community_membership = [i + (community_last + 1) for i in community_membership]
            community_last = max(community_membership)
            df_cluster_data = pd.DataFrame({'id_artist': graph.vs['name'],
                                            'name_artist': graph.vs['name_artist'],
                                            'in_collection': graph.vs['in_collection'],
                                            'id_hierarchy': [tree_level] * qty_vertices,
                                            'id_community_from': graph.vs['id_community_from'],
                                            'id_community': community_membership,
                                            'eigenvalue': eigenvalue})
            lst_cluster_data.append(df_cluster_data)

        qty_graphs_queued = len(lst_processing_queue)
        print("Graphs in queue: " + str(qty_graphs_queued))  # TODO: Remove

    df_data = pd.concat(lst_cluster_data, axis=0, ignore_index=True)
    print("Out of my depth: done!")  # TODO: Remove
    return df_data
 """