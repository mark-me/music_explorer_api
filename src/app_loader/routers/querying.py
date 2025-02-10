from typing import Union

from
import services as _services
import sqlalchemy.orm as _orm
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(
    prefix='/query',
    tags=['DB resources']
)

_services.create_database()

@router.post("/collection_artists/")
def read_collection_artists(
    db: _orm.Session=Depends(_services.get_db),
    ):
    collection_artists = _services.get_collection_artists(db=db)
    return collection_artists

@router.post("/collection_artist_releases/{id_artist}")
def read_collection_artist_releases(
    id_artist: str,
    db: _orm.Session=Depends(_services.get_db),
    ):
    db_collection_artist  = _services.get_collection_artist(db=db, id_artist=id_artist)
    if db_collection_artist is None:
        raise HTTPException(
            status_code=404, detail="Sorry, this artist doesn't exist in your collection"
        )
    collection_releases = _services.get_collection_artist_releases(db=db, id_artist=id_artist)
    return collection_releases

@router.post("/release_videos/{id_release}")
def read_release_videos(
    id_release: int,
    db: _orm.Session=Depends(_services.get_db),
    ):
    release_videos = _services.get_release_videos(db=db, id_release=id_release)
    return release_videos

@router.post("/dendro_vertices/{id_hierarchy}")
def read_dendrogram_vertices(
    id_hierarchy: int,
    db: _orm.Session=Depends(_services.get_db),
    ):
    dendrogram_vertices = _services.get_dendrogram_vertices(db=db, id_hierarchy=id_hierarchy)
    return dendrogram_vertices

@router.post("/dendro_edges/{id_hierarchy}")
def read_dendrogram_edges(
    id_hierarchy: int,
    db: _orm.Session=Depends(_services.get_db),
    ):
    dendrogram_edges = _services.get_dendrogram_edges(db=db, id_hierarchy=id_hierarchy)
    return dendrogram_edges

@router.post("/spinder/")
def read_spinder(
    id_artist: Union[int, None] = None,
    db: _orm.Session=Depends(_services.get_db),
    ):
    print(id_artist)
    if id_artist is None:
        spinder_suggestion = _services.get_spinder_random(db=db)
    else:
        spinder_suggestion = _services.get_spinder_artist(db=db, id_artist=id_artist)
    return spinder_suggestion
