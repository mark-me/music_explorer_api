import os
import datetime as dt
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
        
db = Database(db_file=db_file)
artists = _db_reader.Artists(db_file=db_file)
df_edges = artists.edges()
df_vertices = artists.vertices()[['id_artist', 'name_artist']]
g = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)
#ebs = g.edge_betweenness()
#max_eb = max(ebs)

lst_graphs = g.decompose()
print(len(lst_graphs))
idx = 0
for graph in lst_graphs:
    print([str(idx) + " - "] + graph.vs['name_artist'])
    idx = idx + 1
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