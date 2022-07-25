from fastapi import Query
import sqlalchemy.orm as _orm
from sqlalchemy import or_

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

def get_collection_artist_releases(db: _orm.Session, id_artist: str) -> _orm.Query:
    collection_releases = db.query(_models.CollectionRelease).\
        filter(_models.CollectionRelease.id_artist == id_artist).\
            all()
    return collection_releases

def get_release_videos(db: _orm.Session, id_release: int) -> _orm.Query:
    release_videos = db.query(_models.ReleaseVideo).\
        filter(_models.ReleaseVideo.id_release == id_release).\
            all()
    return release_videos

def get_dendrogram_vertices(db: _orm.Session, id_hierarchy: int) -> _orm.Query:
    vertices = db.query(_models.DendrogramVertices).\
        filter(or_(_models.DendrogramVertices.id_hierarchy == id_hierarchy, _models.DendrogramVertices.id_hierarchy == id_hierarchy + 1)).\
            all()
    return vertices

def get_dendrogram_edges(db: _orm.Session, id_hierarchy: int) -> _orm.Query:
    vertices = db.query(_models.DendrogramEdges).\
        filter(_models.DendrogramEdges.id_hierarchy == id_hierarchy).\
            all()
    return vertices