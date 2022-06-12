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
        db = sqlite3.connect(self.db_file)
        cursor = db.cursor()
        cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='artist' ''')
        has_table = cursor.fetchone()[0]==1 
        if has_table:
            sql_statement = "SELECT DISTINCT collection_artists.id_artist, collection_artists.api_artist \
                             FROM collection_artists \
                             LEFT JOIN artist \
                                ON  artist.id_artist = collection_artists.id_artist \
                             WHERE artist.name_artist IS NULL AND \
                                   collection_artists.api_artist IS NOT NULL AND \
                                   LENGTH(collection_artists.api_artist) > 0;"
            df_artist = pd.read_sql_query(sql_statement, con=db)
        else:
            df_artist = pd.read_sql(sql="SELECT DISTINCT id_artist, api_artist FROM collection_artists", con=db)
        db.close() 
        return df_artist


class Artists(_DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)  
    
    def new_aliases(self) -> pd.DataFrame:
        db = sqlite3.connect(self.db_file)
        sql_statement = "SELECT DISTINCT id_alias AS id_artist, api_alias as api_artist \
                         FROM artist_aliases \
                         WHERE id_alias NOT IN (SELECT id_artist FROM artist) AND\
                               api_alias IS NOT NULL AND LENGTH(api_alias) > 0;"
        return pd.read_sql_query(sql_statement, con=db)

    def new_groups(self) -> pd.DataFrame:
        db = sqlite3.connect(self.db_file)
        sql_statement = "SELECT DISTINCT id_group AS id_artist, api_group as api_artist \
                         FROM artist_groups \
                         WHERE id_group NOT IN (SELECT id_artist FROM artist) AND\
                               api_group IS NOT NULL AND LENGTH(api_group) > 0;"
        return pd.read_sql_query(sql_statement, con=db)

    def new_members(self) -> pd.DataFrame:
        db = sqlite3.connect(self.db_file)
        sql_statement = "SELECT DISTINCT id_member AS id_artist, api_member as api_artist \
                         FROM artist_members \
                         WHERE id_member NOT IN (SELECT id_artist FROM artist) AND\
                               api_member IS NOT NULL AND LENGTH(api_member) > 0;"
        return pd.read_sql_query(sql_statement, con=db)
