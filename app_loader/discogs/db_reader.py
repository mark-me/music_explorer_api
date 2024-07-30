import pandas as pd
import sqlite3

from db_utils import DBStorage


class Collection(DBStorage):
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
                                UNION\
                                    SELECT id_artist FROM release_artists )\
            WHERE id_artist NOT IN ( SELECT id_artist FROM artist ) AND\
					id_artist NOT IN ( SELECT id_artist FROM artist_ignore) AND\
                    id_artist NOT IN ( SELECT id_artist FROM artist_write_attempts WHERE qty_attempts > 1)"
        self.create_view(name_view=name_view, sql_definition=sql_definition)


    def artists_not_added(self) -> pd.DataFrame:
        return self.read_table(name_table='vw_artists_not_added')
        # TODO: Remove
        # db_con = sqlite3.connect(self.db_file)
        # df_artists = pd.read_sql(sql="SELECT * FROM vw_artists_not_added;", con=db_con)
        # db_con.close()
        # return df_artists

    def qty_artists_not_added(self) -> int:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM vw_artists_not_added;")
        qty_artists = cursor.fetchone()[0]
        db_con.close()
        return qty_artists

    def artists_write_attempts(self) -> pd.DataFrame:
        return self.read_table(name_table='artist_write_attempts')

    def artists(self) -> pd.DataFrame:
        return self.read_table(name_table='artist')


class Artists(DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def artists(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_artists = pd.read_sql_query("SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist", con=db_con)
        db_con.close()
        return df_artists

    def vertices(self) -> pd.DataFrame:
        db_con = sqlite3.connect(self.db_file)
        df_vertices = pd.read_sql_query("SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist", con=db_con)
        db_con.close()
        return df_vertices

    def edges(self) -> pd.DataFrame:
        return self.read_table(name_table='artist_relations')

    def community_hierarchy_edges(self) -> pd.DataFrame:
        return self.read_table(name_table='community_dendrogram_edges')

    def community_hierarchy_vertices(self) -> pd.DataFrame:
        return self.read_table(name_table='community_dendrogram_vertices')
