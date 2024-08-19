from discogs.db_utils import DBStorage


class DBTransform(DBStorage):
    def __init__(self, db_file) -> None:
        super().__init__(db_file)
        pass

    def artist_is_groups(self) -> None:
        self.column_add(name_table="artist", name_column="is_group", type_data="INT")
        sql_statement = "CREATE INDEX IF NOT EXISTS idx_artist_members_id_artist ON artist_members (id_artist);"
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist);
        """
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET is_group = 0
        WHERE is_group IS NULL;
        """
        self.execute_sql(sql=sql_statement)

    def artist_thumbnails(self) -> None:
        self.column_add(
            name_table="artist", name_column="url_thumbnail", type_data="VARCHAR"
        )
        self.drop_existing_table(name_table="thumbnails")
        sql_statement = """
        CREATE TABLE thumbnails AS
        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases
        UNION
        SELECT id_member, url_thumbnail FROM artist_members
        UNION
        SELECT id_group, url_thumbnail FROM artist_groups;
        """
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET url_thumbnail = ( SELECT url_thumbnail WHERE id_artist = artist.id_artist );
        """
        self.execute_sql(sql=sql_statement)
        # self.drop_existing_table(name_table="thumbnails")

    def artist_collection_items(self) -> None:
        self.column_add(
            name_table="artist", name_column="qty_collection_items", type_data="INT"
        )

        self.drop_existing_table(name_table="qty_collection_items")
        sql_statement = """
        CREATE TABLE qty_collection_items AS
            SELECT release_artists.id_artist AS id_artist, COUNT(*) AS qty_items
            FROM collection_items
            INNER JOIN release_artists
                ON release_artists.id_release = collection_items.id_release
            GROUP BY release_artists.id_artist;
        """

        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET qty_collection_items = (SELECT qty_items
                                    FROM qty_collection_items
                                    WHERE  qty_collection_items.id_artist = artist.id_artist);
        """
        self.execute_sql(sql=sql_statement)
        self.drop_existing_table(name_table="qty_collection_items")

    def load_roles(self) -> None:
        has_table = self.table_exists(name_table="role")
        if not has_table:
            sql_statement = """
            CREATE TABLE role AS
                SELECT DISTINCT
                    role,
                    0 AS as_edge
                FROM release_credits
            """
            self.execute_sql(sql=sql_statement)
        list_value_part = [
            "piano", "vocal", "perform", "bass", "viol", "drum", "keyboard", "guitar", "sax",
            "music", "written", "arrange", "lyric", "word", "compose", "song", "accordion",
            "chamberlin", "clarinet", "banjo", "band", "bongo", "bell", "bouzouki", "brass",
            "cello", "cavaquinho", "celest", "choir", "chorus", "handclap", "conduct", "conga",
            "percussion", "trumpet", "cornet", "djembe", "dobro", "organ", "electron", "horn",
            "fiddle", "flute", "recorder", "glocken", "gong", "guest", "vibra", "harmonium",
            "harmonica", "harp", "beatbox", "leader", "loop", "MC", "mellotron", "melod",
            "mixed", "oboe", "orchestra", "recorded", "remix", "saw", "score", "sitar",
            "strings", "synth", "tabla", "tambourine", "theremin", "timbales", "timpani",
            "whistle", "triangle", "trombone", "tuba", "vocoder", "voice", "phone", "woodwind",
        ]
        last_value = list_value_part.pop(-1)
        sql_start = "UPDATE role SET as_edge = 1 WHERE "
        sql_statement = (
            sql_start
            + "".join(f"""role LIKE "%{value_part}%" OR """ for value_part in list_value_part)
            + f"""role LIKE "%{last_value}%" """
        )
        self.execute_sql(sql=sql_statement)

    def artist_relationships(self) -> None:
        self.drop_existing_table(name_table="artist_relations")
        sql_statement = """
        CREATE TABLE artist_relations AS
            SELECT DISTINCT id_artist_from,
                            id_artist_to,
                            relation_type
            FROM (  SELECT id_member AS id_artist_from,
                        id_artist as id_artist_to,
                        'group_member' as relation_type
                    FROM artist_members
                UNION
                    SELECT id_artist as id_artist_from,
                        id_group AS id_artist_to,
                        'group_member' as relation_type
                    FROM artist_groups
                UNION
                    SELECT a.id_alias,
                        a.id_artist,
                        'artist_alias'
                    FROM artist_aliases a
                    LEFT JOIN artist_aliases b
                        ON a.id_artist = b.id_alias AND
                            a.id_alias = b.id_artist
                    WHERE a.id_artist > b.id_artist OR
                        b.id_artist IS NULL
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'co_appearance'
                    FROM release_artists a
                    INNER JOIN release_artists b
                        ON b.id_release = a.id_release
                    WHERE a.id_artist != b.id_artist
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'release_role'
                    FROM release_artists a
                    INNER JOIN release_credits b
                        ON b.id_release = a.id_release
                    INNER JOIN role c
                        ON c.role = b.role
                    WHERE a.id_artist = b.id_artist AND
                        as_edge = 1
                )
            INNER JOIN artist a
                ON a.id_artist = id_artist_from
            INNER JOIN artist b
                ON b.id_artist = id_artist_to
            GROUP BY id_artist_from,
                id_artist_to,
                relation_type;
        """
        self.execute_sql(sql=sql_statement)

    def artist_vertices(self) -> None:
        """Retrieve artists in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_vertex")
        sql_statement = """
            CREATE TABLE artist_vertex AS
                SELECT id_artist, MAX(in_collection) AS in_collection
                FROM (  SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist
                            UNION
                        SELECT id_alias, 0 as in_collection from artist_aliases
                            UNION
                        SELECT id_member, 0 FROM artist_members
                            UNION
                        SELECT id_group, 0 FROM artist_groups
                            UNION
                        SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')
                            UNION
                        SELECT release_artists.id_artist, MAX(IIF(date_added IS NULL, 0, 1))
                        FROM release_artists
                        INNER JOIN release
                            ON release.id_release = release_artists.id_release
                        LEFT JOIN collection_items
                            ON collection_items.id_release = release.id_release
                        GROUP BY release_artists.id_artist )
                GROUP BY id_artist"""
        self.execute_sql(sql=sql_statement)

    def artist_edges(self) -> None:
        """Retrieve artist cooperations in order to determine where to stop discogs extraction"""
        self.drop_existing_table(name_table="artist_edge")
        sql_statement = """
            CREATE TABLE artist_edge AS
                SELECT DISTINCT id_artist_from, id_artist_to, relation_type
                FROM (  SELECT id_member AS id_artist_from, id_artist as id_artist_to, 'group_member' as relation_type
                        FROM artist_members
                    UNION
                        SELECT id_artist, id_group, 'group_member' FROM artist_groups
                    UNION
                        SELECT a.id_alias, a.id_artist, 'artist_alias'
                        FROM artist_aliases a
                        LEFT JOIN artist_aliases b
                            ON a.id_artist = b.id_alias AND
                                a.id_alias = b.id_artist
                        WHERE a.id_artist > b.id_artist OR b.id_artist IS NULL
                    UNION
                        SELECT a.id_artist, b.id_artist, 'co_appearance'
                        FROM release_artists a
                        INNER JOIN release_artists b
                            ON b.id_release = a.id_release
                        WHERE a.id_artist != b.id_artist )
                GROUP BY id_artist_from, id_artist_to, relation_type;"""
        self.execute_sql(sql=sql_statement)

    def artist_graph(self) -> None:
        pass

    def start(self) -> None:
        self.artist_is_groups()
        self.artist_thumbnails()
        self.artist_collection_items()
        self.load_roles()
        self.artist_relationships()
        self.artist_vertices()
        self.artist_edges()
        self.artist_graph()
