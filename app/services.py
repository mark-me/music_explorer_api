import sqlalchemy.orm as _orm

import database as _database, models as _models, schemas as _schemas


def create_database():
    db_file = '/data/music_collection.db'
    db_collection = _database.ViewCollection(db_file=db_file)
    db_collection.artist_collection_items()
    db_collection.artists_in_collection()
    return _database.Base.metadata.create_all(bind=_database.engine)

def get_db():
    db = _database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_collection_artist(db: _orm.Session, id_artist: str) -> _orm.Query:
    return db.query(_models.CollectionArtist).filter(_models.CollectionArtist.id_artist == id_artist).first()

def get_collection_artists(db: _orm.Session) -> _orm.Query:
    collection_artists = db.query(_models.CollectionArtist).all()
    return collection_artists

def get_collection_artist_releases(db: _orm.Session, id_artist: str):
    collection_releases = db.query(_models.CollectionRelease).filter(_models.CollectionRelease.id_artist == id_artist).all()
    return collection_releases

def get_release_videos(db: _orm.Session, id_release: int) -> _orm.Query:
    release_videos = db.query(_models.ReleaseVideo).filter(_models.ReleaseVideo.id_release == id_release).all()
    return release_videos