import yaml
import sqlalchemy as _sql
import sqlalchemy.ext.declarative as _declarative
import sqlalchemy.orm as _orm
import sqlite3

with open(r'config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
db_file = config['db_file']

DATABASE_URL = "sqlite:///" + db_file

engine = _sql.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)

SessionLocal = _orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = _declarative.declarative_base()


class _BaseViewCreator:
    def __init__(self, db_file: str) -> None:
        self.db_file = db_file
    
    def create(self, name_view:str, sql_definition: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        sql = "CREATE VIEW " + name_view + " AS " + sql_definition +";"
        cursor.execute(sql)
        db_con.commit()
        db_con.close()
    
    def drop(self, name_view: str) -> None:
        db_con = sqlite3.connect(self.db_file)
        cursor = db_con.cursor()
        cursor.execute("DROP VIEW IF EXISTS " + name_view)
        db_con.commit()
        db_con.close()

class ViewCollection(_BaseViewCreator):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file=db_file)
    
    def artists_in_collection(self) -> None:
        name_view = 'vw_artists_qty_in_collection'
        self.drop(name_view=name_view)
        sql_definition = "SELECT artist.id_artist, name_artist, artist_images.url_image AS url_image, COUNT(*) AS qty_collection_items\
            FROM artist\
            LEFT JOIN artist_images\
                ON artist_images.id_artist = artist.id_artist\
            INNER JOIN release_artists\
                ON artist.id_artist = release_artists.id_artist\
            INNER JOIN release\
                ON release.id_release = release_artists.id_release\
            INNER JOIN collection_items\
                ON collection_items.id_release = release.id_release\
            WHERE artist_images.type = 'primary' OR artist_images.type IS NULL\
            GROUP BY artist.id_artist, name_artist, artist_images.url_image\
            ORDER BY COUNT(*) DESC"
        self.create(name_view=name_view, sql_definition=sql_definition)
        
    def artist_collection_items(self) -> None:
        name_view = 'vw_artist_collection_releases'
        self.drop(name_view=name_view)
        sql_definition = "SELECT DISTINCT release_artists.id_artist, collection_items.id_release,\
                name_artist, release.title AS name_release, release.url_cover\
            FROM collection_items\
            INNER JOIN release_artists\
                ON release_artists.id_release = collection_items.id_release\
            INNER JOIN artist\
                ON artist.id_artist = release_artists.id_artist\
            INNER JOIN release\
                ON release.id_release = release_artists.id_release"
        self.create(name_view=name_view, sql_definition=sql_definition)


class Artist(_BaseViewCreator):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file=db_file)
        
        
class Release(_BaseViewCreator):
    def __init__(self, db_file: str) -> None:
        super().__init__(db_file=db_file)