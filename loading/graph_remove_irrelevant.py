import itertools
import yaml
import sqlite3
import numpy as np
import pandas as pd
import igraph as ig
import networkx as nx
from pyvis.network import Network
from tqdm import tqdm
from collections import Counter

import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader

with open(r'config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
db_file = config['db_file']

class Database(_db_reader._DBStorage):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file)

    def extract_artist_edges(self) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql_file = open("loading/sql/extract_artist_relations.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)
        
def artist_vertices(db_file: str) -> pd.DataFrame:
    sql = "SELECT id_artist, MAX(in_collection) AS in_collection\
            FROM (\
                SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist\
                UNION \
                SELECT id_alias, 0 as in_collection from artist_aliases\
                UNION\
                SELECT id_member, 0 FROM artist_members\
                UNION\
                SELECT id_group, 0 FROM artist_groups\
                UNION\
                SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')\
            )\
            GROUP BY id_artist"
    db_con = sqlite3.connect(db_file)
    df_vertices = pd.read_sql_query(sql=sql, con=db_con)
    db_con.close() 
    return df_vertices

artists = _db_reader.Artists(db_file=db_file)
df_edges = artists.edges()
df_vertices = artist_vertices(db_file=db_file)
graph_all = ig.Graph.DataFrame(edges=df_edges, directed=False, vertices=df_vertices)

vtx_collection = graph_all.vs.select(in_collection_eq=1)
vtx_connectors_all = []
for vtx in vtx_collection:
    # Only query those that have less than 3 steps
    vtx_neighbors = graph_all.neighborhood(vertices=vtx, order=2)
    vtx_neighbors = list(set(vtx_neighbors))
    vtx_connectors_all = list(set(vtx_neighbors + vtx_connectors_all))
    # Integrate connectors
    vtx_connectors = graph_all.get_shortest_paths(vtx, to=vtx_collection)
    vtx_connectors = [x for l in vtx_connectors for x in l]
    vtx_connectors_all = list(set(vtx_connectors + vtx_connectors_all))
    
vtx = ig.VertexSeq(graph_all, vtx_connectors_all)
vtx['color'] = 'red'
print(Counter(graph_all.vs['color']))

vtx_to_exclude = list(set(graph_all.vs.indices) - set(vtx_connectors_all))
print("Number of vertices to consider: " + str(len(vtx_connectors_all)))
print("Number of vertices to exclude: " + str(len(vtx_to_exclude)))

df_include = pd.DataFrame({'id_artist': graph_all.vs[vtx_connectors_all]['name']})
df_ignore = pd.DataFrame({'id_artist': graph_all.vs[vtx_to_exclude]['name']})
db_writer = _db_writer.Artists(db_file=db_file)
db_writer.ignore_list(df_ignore=df_ignore)

# Effects on the graph components
graph_less = graph_all.copy()
graph_less.delete_vertices(vtx_to_exclude)

lst_all = graph_all.decompose()
lst_less = graph_less.decompose()
print("Complete graph, number of components: " + str(len(lst_all)))
print("Reduced graph, number of components: " + str(len(lst_less)))


