from .utils import DBStorage

class CollectionReader(DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)

    def artists(self, id_artist: str=None) -> list:
        name_source = "vw_artists_qty_in_collection"
        if id_artist is None:
            lst_rows = self.read_table(name_table=name_source)
        else:
            sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
            lst_rows = self.read_sql(sql=sql)
        return lst_rows

    def releases(self, id_artist: str=None) -> list:
        name_source = "vw_artist_collection_releases"
        if id_artist is None:
            lst_rows = self.read_table(name_table=name_source)
        else:
            sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
            lst_rows = self.read_sql(sql=sql)
        return lst_rows

    def release_videos(self, id_release: int) -> list:
        name_source = "release_videos"
        sql = f"SELECT * FROM {name_source} WHERE id_release={id_release}"
        lst_rows = self.read_sql(sql=sql)
        return lst_rows

    def dendrogram_vertices(self, id_hierarchy: int) -> list:
        name_source = "community_dendrogram_vertices"
        sql = f"SELECT * FROM {name_source} WHERE id_hierarchy={id_hierarchy + 1}"
        lst_rows = self.read_sql(sql=sql)
        return lst_rows

    def dendrogram_edges(self, id_hierarchy: int) -> list:
        name_source = "community_dendrogram_edges"
        sql = f"SELECT * FROM {name_source} WHERE id_hierarchy={id_hierarchy}"
        lst_rows = self.read_sql(sql=sql)
        return lst_rows

    def spindler_random(self) -> list:
        name_source = "vw_spinder_artist"
        lst_rows = self.read_table(name_table=name_source)
        return lst_rows

    def spindler_artist(self, id_artist: str) -> list:
        name_source = "vw_spinder_artist"
        sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
        lst_rows = self.read_sql(sql=sql)
        return lst_rows

