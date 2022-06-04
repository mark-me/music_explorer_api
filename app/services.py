import sqlalchemy.orm as _orm

import database as _database, models as _models, schemas as _schemas


def create_database():
    return _database.Base.metadata.create_all(bind=_database.engine)


def get_db():
    db = _database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_collection_artists(db: _orm.Session, skip:int, limit:int):
    collection_artists = db.query(_models.CollectionArtist).offset(skip).limit(limit).all()
    return collection_artists
    