import sqlite3
import numpy as np
import pandas as pd

class _DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file


class Collection(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def artists_not_added(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_artist = pd.read_sql(sql="SELECT * FROM vw_artists_not_added;", con=db_con)
        db_con.close() 
        return df_artist

    def qty_artists_not_added(self) -> int:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM vw_artists_not_added;")
        qty_artists = cursor.fetchone()[0]
        db_con.close()
        return qty_artists


class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def vertices(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_vertices = pd.read_sql_query("SELECT * FROM artist", con=db_con)
        db_con.close() 
        return df_vertices
    
    def edges(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_edges = pd.read_sql_query("SELECT * FROM vw_artist_edges", con=db_con)
        db_con.close() 
        return df_edges
