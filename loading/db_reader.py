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
        cursor = db_con.cursor()
        df_artist = pd.read_sql(sql="SELECT * FROM vw_artists_not_added;", con=db_con)
        db_con.close() 
        return df_artist

    def qty_artists_not_added(self) -> int:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql = "SELECT COUNT(*) FROM vw_artists_not_added;"
        cursor.execute(sql)
        return cursor.fetchone()[0]


class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def vertices(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        sql_statement = "SELECT * FROM artist"
        return pd.read_sql_query(sql_statement, con=db_con)
    
    def edges(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        return pd.read_sql_query("SELECT * FROM vw_artist_edges", con=db_con)
