import sys
#import os
import datetime as dt
import time
import yaml
import numpy as np
import pandas as pd
import igraph as ig
from colour import Color
import collections

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
            FROM (\
                    SELECT id_member AS id_artist_from,\
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
df_vertices = artists.vertices()[['id_artist', 'name_artist']]
graph_all = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)

# Decompose graph
lst_graphs = graph_all.decompose()
print(len(lst_graphs))
idx = 0
for graph in lst_graphs:
    print([str(idx) + " - " + str(len(graph.vs))]) #  + graph.vs['name_artist']
    idx = idx + 1

""" Pseudo code for loop
* Create a list with decomposed graphs
* For each sub graph until none in the list:
    * Determine communities
    * If the number of communities suggested is larger than 15, limit to 15
    * If the number of communities is 5 more or less than the total number of vertices in the graph:
        * Set the community number of each vertex with a sequential number
        * Set the eigenvalue of each of the vertices to 1
    * Else
        * Set the community numbers of the vertices
        * Determine eigenvalue of vertices
        * Create subgraph for each community and add to list
    * Remove subgraph
"""

def plot_graph(graph: ig.Graph, filename: str) -> None:
    colors = ['#F0A0FF', '#0075DC', '#993F00', '#4C005C', '#191919', '#005C31', '#2BCE48', '#FFCC99', '#808080', '#94FFB5', '#8F7C00',
            '#9DCC00', '#C20088', '#003380', '#FFA405', '#FFA8BB', '#426600', '#FF0010', '#5EF1F2', '#00998F', '#E0FF66', '#740AFF',
            '#990000', '#FFFF80', '#FFE100', '#FF5005']
    graph.vs['color']
    graph.write_svg(filename)

graph = lst_graphs[5].copy()            # Start point for tree probing
graph.vs['id_community_from'] = [0] * len(graph.vs) 
# Queue for processing graphs, keeping track level in community tree and relationships between branches
lst_processing_queue = [{'graph': graph, 'tree_level': 0}]

# For each sub graph until none in the list
qty_graphs_queued = len(lst_processing_queue)     # Number of graphs in the community tree
lst_cluster_data = []                   # Data-frames with data for a processed graph
df_cluster_relations = []               # TODO: Remove
community_last = 0
while qty_graphs_queued > 0:
    dict_queue = lst_processing_queue.pop(0)
    graph = dict_queue.get('graph')
    tree_level = dict_queue.get('tree_level')
    
    qty_vertices = len(graph.vs)
    print("Tree depth: " + str(tree_level) + " - Total graph # vertices: " + str(len(graph.vs))) # TODO: Remove
    # Only cluster if the number of vertices is higher than 15
    if qty_vertices > 15:
        # Cluster
        tic = time.perf_counter()
        cluster_result = graph.community_edge_betweenness(directed=False)
        # Setting maximum and minimum of number of clusters
        qty_clusters = 15 if cluster_result.optimal_count > 15 else cluster_result.optimal_count
        qty_clusters = qty_clusters if qty_clusters > 4 else 4
        # Determine clusters
        community_membership = cluster_result.as_clustering(n=qty_clusters).membership
        
        qty_vertices_sub = 0  # TODO: Remove
        communities = set(community_membership)
        eigenvalue = []
        for community in communities:
            idx_not_in_community = np.where(np.array(community_membership) != community)[0].tolist() # Determine vertices not in community
            graph_sub = graph.copy()
            graph_sub.delete_vertices(idx_not_in_community)
            graph_sub.vs['id_community_from'] = [community] * len(graph_sub.vs)

            print("Community " + str(community) + " has " + str(len(graph_sub.vs)) + " vertices")  # TODO: Remove
            qty_vertices_sub = qty_vertices_sub + len(graph_sub.vs)  # TODO: Remove
            lst_processing_queue.append({'graph': graph_sub.copy(), 
                                         'tree_level': tree_level + 1})
            
            # TODO: Calculate eigenvalue per sub_graph, but include in graph results
            eigenvalue = eigenvalue + graph_sub.eigenvector_centrality(directed=False)
        print("Vertices processed: " + str(qty_vertices_sub)) # TODO: Remove
    else:
        community_membership = range(0, qty_vertices)
        eigenvalue = [1] * qty_vertices
        
    # Make sure community numbers are unique    
    community_membership = [i + (community_last + 1) for i in community_membership]
    community_last = max(community_membership)
        
    df_cluster_data = pd.DataFrame({'id_artist': graph.vs['name'],
                                    'name_artist': graph.vs['name_artist'],
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
print("Out of my depth: done!")


df_community_edges = artists.community_hierarchy_edges()
df_community_vertices = artists.community_hierarchy_vertices()

graph_community = ig.Graph.DataFrame(edges=df_community_edges, 
                                     directed=False,
                                     vertices=df_community_vertices)

colors = ['#F0A0FF', '#0075DC', '#993F00', '#4C005C', '#191919', '#005C31', '#2BCE48', '#FFCC99', '#808080', '#94FFB5', '#8F7C00',
            '#9DCC00', '#C20088', '#003380', '#FFA405', '#FFA8BB', '#426600', '#FF0010', '#5EF1F2', '#00998F', '#E0FF66', '#740AFF',
            '#990000', '#FFFF80', '#FFE100', '#FF5005']
graph_community.vs['label'] = graph_community.vs['name']
graph_community.vs['color'] = [colors[i] for i in graph_community.vs['id_hierarchy']]

is_a_tree = graph_community.is_tree()

graph_community.write_svg('graph_community.svg', layout='reingold_tilford_circular', width=1600, height=800)

sys.exit()
"""    
g_test = lst_graphs[6]
g_test.vs['id_artist'] = g_test.vs['name']
print(g_test.vs['name_artist'] == None)
g_test.vs['label'] = g_test.vs['name_artist']
#g_test.es['label'] = g_test.edge_betweenness()
g_test.write_svg('test.svg')
#print(g_test.es['label']) """

lst_color = ['#a6cee3','#1f78b4','#b2df8a','#33a02c','#fb9a99','#e31a1c','#fdbf6f','#ff7f00','#cab2d6','#6a3d9a','#ffff99','#b15928']
g_cluster_test = lst_graphs[3]
# Prevent invalid plot names
names = g_cluster_test.vs['name_artist']
names2 = [i.replace("&", "and") for i in names]
g_cluster_test.vs['label'] = names2

# 1st Iteration
# Cluster the graph
clusters = g_cluster_test.community_edge_betweenness(directed=False)
# Cut the cluster hierarchy to determine membership
iter_cluster = 0
membership = clusters.as_clustering(n=10)
communities = set(membership.membership)
lst_sub_graphs = []
lst_data = []
#for iter_hierarchy in range(1, 10):
for iter_community in communities:
    # Make a subgraph of the community
    array = np.array(membership.membership)
    bool_array = array != iter_community
    idx = np.where(bool_array)[0].tolist()
    graph_sub = g_cluster_test.copy()
    graph_sub.delete_vertices(idx)
    lst_sub_graphs.append(graph_sub.copy())
    
    # Calculate the eigenvalue of the community members within the community
    eigenvalue = graph_sub.eigenvector_centrality(directed=False)
    # Subgraph clustering
    sub_clusters = graph_sub.community_edge_betweenness(directed=False)
    sub_membership = sub_clusters.as_clustering()
    
    # Collect data and store to DB
    df_cluster = pd.DataFrame({'id_artist': graph_sub.vs['name'],
                                'name_artist': graph_sub.vs['name_artist'], 
                                'eigenvalue': eigenvalue,
                                'sub_community': sub_membership.membership})
    df_cluster['community'] = iter_community
    df_cluster['hierarchy'] = iter_cluster
    lst_data.append(df_cluster)
df_data = pd.concat(lst_data, axis=0, ignore_index=True)          
        
# Write data
g_cluster_test.write_svg('clusters_' + str(iter_cluster) + '.svg')



print("Number of artists per cluster : ")
for key, value in  dict(collections.Counter(test.membership)).items():
    print(key, ' : ', value)
#g_cluster_test.vs['color'] = [lst_color[i] for i in test.membership]


array = np.array(test.membership)
bool_array = array != 0
idx = np.where(bool_array)[0].tolist()
g_cluster_test.delete_vertices(idx)

eigenvalue = g_cluster_test.eigenvector_centrality(directed=False)
color_yellow = Color("yellow")
color_range = list(color_yellow.range_to(Color("red"),11))
bin_eigenvalue = [round(i * 10) for i in eigenvalue]
g_cluster_test.vs['color'] = [color_range[i] for i in bin_eigenvalue]
g_cluster_test.vs['eigenvalue'] = eigenvalue
g_cluster_test.vs.select(lambda vertex: vertex['eigenvalue'] < .8)['label'] = ''
#g_cluster_test.vs[ids]['label'] = ''
df_vertices = pd.DataFrame({'names': g_cluster_test.vs['name_artist'], 'eigenvalue': eigenvalue})
g_cluster_test.write_svg('clusters2.svg') 
print("Done")