import sys
import datetime as dt
import time
import yaml
import numpy as np
import pandas as pd
import igraph as ig
import networkx as nx
from pyvis.network import Network

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader

with open(r'config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
db_file = config['db_file']

class Database(_db_reader._DBStorage):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file)
        self.create_edges_view()

    def create_edges_view(self) -> None:
        name_view = 'vw_artist_edges'
        self.drop_view(name_view=name_view)
        sql_definition = "SELECT DISTINCT id_artist_from,\
                id_artist_to,\
                relation_type\
            FROM (  SELECT id_member AS id_artist_from,\
                        id_artist as id_artist_to,\
                        'group_member' as relation_type\
                    FROM artist_members\
                UNION\
                    SELECT id_artist as id_artist_from,\
                        id_group AS id_artist_to,\
                        'group_member' as relation_type\
                    FROM artist_groups\
                UNION\
                    SELECT a.id_alias,\
                        a.id_artist,\
                        'artist_alias'\
                    FROM artist_aliases a\
                    LEFT JOIN artist_aliases b\
                        ON a.id_artist = b.id_alias AND\
                            a.id_alias = b.id_artist\
                    WHERE a.id_artist > b.id_artist OR\
                        b.id_artist IS NULL\
                )\
            INNER JOIN artist a\
                ON a.id_artist = id_artist_from\
            INNER JOIN artist b\
                ON b.id_artist = id_artist_to\
            GROUP BY id_artist_from,\
                id_artist_to,\
                relation_type"
        self.create_view(name_view=name_view, sql_definition=sql_definition)

# Create overall graph        
db = Database(db_file=db_file)
artists = _db_reader.Artists(db_file=db_file)
df_edges = artists.edges()
df_vertices = artists.vertices()[['id_artist', 'name_artist', 'in_collection']]
graph_all = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)

# Decompose graph
lst_graphs = graph_all.decompose()
# TODO: Remove
print(len(lst_graphs))
idx = 0
for graph in lst_graphs:
    print([str(idx) + " - " + str(len(graph.vs))]) #  + graph.vs['name_artist']
    idx = idx + 1

def cluster_artist_graph(graph: ig.Graph) -> None:
    # Queue for processing graphs, keeping track level in community tree and relationships between branches
    lst_processing_queue = [{'graph': graph, 'tree_level': 0}]

    # For each sub graph until none in the list
    qty_graphs_queued = len(lst_processing_queue)   # Number of graphs in the community tree
    lst_cluster_data = []                           # Data-frames with data for a processed graph
    community_last = 0
    while qty_graphs_queued > 0:
        dict_queue = lst_processing_queue.pop(0)
        graph = (dict_queue.get('graph')).simplify()
        tree_level = dict_queue.get('tree_level')
        
        qty_collection_items = sum(graph.vs['in_collection'])
        qty_vertices = len(graph.vs)
        print("Tree depth: " + str(tree_level) + " - Total graph # vertices: " + str(len(graph.vs))) # TODO: Remove
        # Only cluster if the number of vertices is higher than 15
        if qty_collection_items > 15:
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
                graph_sub.vs['id_community_from'] = [community] * len(graph_sub.vs)
                print("Community " + str(community) + " has " + str(len(graph_sub.vs)) + " vertices")  # TODO: Remove
                qty_vertices_sub = qty_vertices_sub + len(graph_sub.vs)  # TODO: Remove
                lst_processing_queue.append({'graph': graph_sub.copy(), 'tree_level': tree_level + 1})
                # Calculate eigenvalue per sub_graph
                eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(directed=False)
            print("Vertices processed: " + str(qty_vertices_sub)) # TODO: Remove
        else:
            community_membership = [0] * qty_vertices
            eigenvalue = [1] * qty_vertices
            
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
    db_writer = _db_writer.ArtistNetwork(db_file=db_file)
    db_writer.cluster_hierarchy(df_hierarchy=df_data)
    print("Out of my depth: done!")  # TODO: Remove

def plot_cluster_tree() -> None:
    df_community_edges = artists.community_hierarchy_edges()
    df_community_vertices = artists.community_hierarchy_vertices()
    graph_community = ig.Graph.DataFrame(edges=df_community_edges, 
                                        directed=False,
                                        vertices=df_community_vertices)
    colors = ['#F0A0FF', '#0075DC', '#993F00', '#4C005C', '#191919', '#005C31', '#2BCE48', '#FFCC99', '#808080', '#94FFB5', '#8F7C00',
                '#9DCC00', '#C20088', '#003380', '#FFA405', '#FFA8BB', '#426600', '#FF0010', '#5EF1F2', '#00998F', '#E0FF66', '#740AFF',
                '#990000', '#FFFF80', '#FFE100', '#FF5005']
    graph_community.vs['label'] = graph_community.vs['qty_artists_collection']
    # graph_community.vs['color'] = [colors[i] for i in graph_community.vs['id_hierarchy']]
    #graph_community.write_svg('graph_community.svg', layout='reingold_tilford_circular', width=1600, height=800)
    graph_community.write_svg('graph_community.svg', layout='sugiyama', width=1600, height=800)

def plot_interactive() -> None:
    df_community_edges = artists.community_hierarchy_edges()
    df_community_vertices = artists.community_hierarchy_vertices()
    max_qty = max(df_community_vertices['qty_artists_collection'])
    min_qty = min(df_community_vertices['qty_artists_collection'])
    df_community_vertices['group'] = [1 if i > 0 else 2 for i in df_community_vertices['qty_artists_collection']]
    df_community_vertices['size'] = [round((i - min_qty)/(max_qty - min_qty)* 100 + 10) for i in df_community_vertices['qty_artists_collection']]
    df_community_vertices['label'] = [str(i) for i in df_community_vertices['qty_artists_collection']]
    df_community_vertices['title'] = [str(i) for i in df_community_vertices['qty_artists']]
    df_community_vertices['title'] = '# Artists: ' + df_community_vertices['title'].map(str) + '\n' +\
        '# Artists in collection: ' + df_community_vertices['label'].map(str)
    network = nx.from_pandas_edgelist(df_community_edges, source = 'id_from', target = 'id_to')
    
    node_attr = df_community_vertices.set_index('id_community').to_dict('index')
    nx.set_node_attributes(network, node_attr)
    visnet = Network(height = "100%", width = "100%", bgcolor='#222222', font_color='white', layout='hierarchical')
    visnet.from_nx(network)
    visnet.show('graph_community.html')


graph = lst_graphs[2].copy()
graph.vs['id_community_from'] = [0] * len(graph.vs) 
#cluster_artist_graph(graph=graph)            # Start point for tree probing
#plot_cluster_tree()
plot_interactive()

sys.exit()
