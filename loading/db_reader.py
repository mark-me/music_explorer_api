import sqlite3
import numpy as np
import pandas as pd

class _DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file
        
    def create_view(self, name_view:str, sql_definition: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql = "CREATE VIEW " + name_view + " AS " + sql_definition +";"
        cursor.execute(sql)
        db_con.commit()
        db_con.close()
    
    def drop_view(self, name_view: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("DROP VIEW IF EXISTS " + name_view)
        db_con.commit()
        db_con.close()


class Collection(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)
        self.create_view_artists_not_added()
        
    def create_view_artists_not_added(self) -> None:
        name_view = 'vw_artists_not_added'
        self.drop_view(name_view=name_view)
        sql_definition = "SELECT DISTINCT id_artist\
            FROM (\
                SELECT id_artist FROM artist_masters\
                WHERE role IN ('Main', 'Appearance', 'TrackAppearance')\
                UNION\
                    SELECT id_alias FROM artist_aliases\
                    UNION\
                        SELECT id_member FROM artist_members\
                        UNION\
                            SELECT id_group FROM artist_groups\
                )\
            WHERE id_artist NOT IN ( SELECT id_artist FROM artist )"
        self.create_view(name_view=name_view, sql_definition=sql_definition)


    def artists_not_added(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_artists = pd.read_sql(sql="SELECT * FROM vw_artists_not_added;", con=db_con)
        db_con.close() 
        return df_artists

    def qty_artists_not_added(self) -> int:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM vw_artists_not_added;")
        qty_artists = cursor.fetchone()[0]
        db_con.close()
        return qty_artists
    
    def artists(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_artists = pd.read_sql(sql="SELECT * FROM artist;", con=db_con)
        db_con.close() 
        return df_artists       


class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def artists(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_vertices = pd.read_sql_query("SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist", con=db_con)
        db_con.close() 
        return df_vertices

    def vertices(self) -> pd.DataFrame:
        return self.artists()
    
    def edges(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_edges = pd.read_sql_query("SELECT * FROM vw_artist_edges", con=db_con)
        db_con.close() 
        return df_edges

    def community_hierarchy_edges(self) -> pd.DataFrame:
        sql = "SELECT * FROM community_dedrogram_edges"
        db_con = sqlite3.connect(self.db_file)
        df_data = pd.read_sql_query(sql, con=db_con)
        db_con.close()
        return df_data    
    
    def community_hierarchy_vertices(self) -> pd.DataFrame:
        sql = "SELECT * FROM community_dedrogram_vertices"
        db_con = sqlite3.connect(self.db_file)
        df_data = pd.read_sql_query(sql, con=db_con)
        db_con.close()
        return df_data
