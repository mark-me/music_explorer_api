import sqlite3
import numpy as np
import pandas as pd

class _DBStorage():
    def __init__(self, db_file) -> None:
        self.db_file = db_file


class Collection(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def new_artists(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='artist'")
        has_table = cursor.fetchone()[0]==1 
        if has_table:
            sql_statement = "SELECT DISTINCT collection_artists.id_artist, collection_artists.api_artist \
                             FROM collection_artists \
                             LEFT JOIN artist \
                                ON  artist.id_artist = collection_artists.id_artist \
                             WHERE artist.name_artist IS NULL AND \
                                   collection_artists.api_artist IS NOT NULL AND \
                                   LENGTH(collection_artists.api_artist) > 0;"
            df_artist = pd.read_sql_query(sql_statement, con=db_con)
        else:
            df_artist = pd.read_sql(sql="SELECT DISTINCT id_artist, api_artist FROM collection_artists", con=db_con)
        db_con.close() 
        return df_artist


class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)  

    def new_aliases(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        return pd.read_sql_query("SELECT * FROM vw_alias_no_artist", con=db_con)

    def new_members(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        return pd.read_sql_query("SELECT * FROM vw_member_no_artist", con=db_con)

    def new_groups(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        return pd.read_sql_query("SELECT * FROM vw_group_no_artist", con=db_con)

    def get_qty_artis_not_added(self) -> int:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql = "SELECT COUNT(*)\
                FROM (  SELECT DISTINCT id_member, api_member\
                        FROM artist_members\
                        UNION\
                        SELECT DISTINCT id_alias, api_alias\
                        FROM artist_aliases\
                        UNION\
                        SELECT DISTINCT id_group, api_group\
                        FROM artist_groups)\
                        WHERE id_member NOT IN (SELECT id_artist FROM artist) AND\
                                api_member IS NOT NULL AND LENGTH(api_member) > 0;"
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def vertices(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        sql_statement = "SELECT * FROM artist"
        return pd.read_sql_query(sql_statement, con=db_con)
    
    def edges(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        return pd.read_sql_query("SELECT * FROM vw_artist_edges", con=db_con)
