from typing import Union

from fastapi import APIRouter, HTTPException

from app_loader.db_operations import CollectionReader

router_query = APIRouter(prefix="/db_query", tags=["DB Querying"])

collection = CollectionReader(file_db="/data/music_collection.db")


@router_query.post("/collection_artists/")
def read_collection_artists():
    collection_artists = collection.artists()
    return collection_artists


@router_query.post("/collection_artist_releases/{id_artist}")
def read_collection_artist_releases(id_artist: str):
    lst_artist = collection.artists(id_artist=id_artist)
    if lst_artist is None:
        raise HTTPException(
            status_code=404,
            detail="Sorry, this artist doesn't exist in your collection",
        )
    lst_releases = collection.releases(id_artist=id_artist)
    return lst_releases


@router_query.post("/release_videos/{id_release}")
def read_release_videos(id_release: int):
    lst_release_videos = collection.release_videos(id_release=id_release)
    return lst_release_videos


@router_query.post("/dendro_vertices/{id_hierarchy}")
def read_dendrogram_vertices(id_hierarchy: int):
    lst_dendrogram_vertices = collection.dendrogram_vertices(id_hierarchy=id_hierarchy)
    return lst_dendrogram_vertices


@router_query.post("/dendro_edges/{id_hierarchy}")
def read_dendrogram_edges(id_hierarchy: int):
    lst_dendrogram_edges = collection.dendrogram_edges(id_hierarchy=id_hierarchy)
    return lst_dendrogram_edges


@router_query.post("/spinder/")
def read_spinder(id_artist: Union[int, None] = None):
    if id_artist is None:
        lst_spinder_suggestion = collection.spindler_random()
    else:
        lst_spinder_suggestion = collection.spindler_artist(id_artist=id_artist)
    return lst_spinder_suggestion
