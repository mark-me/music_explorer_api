from typing import List
from typing import Union

import uvicorn

import fastapi as _fastapi
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy.orm as _orm
import services as _services

app = _fastapi.FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

_services.create_database()

@app.post("/collection_artists/")
def read_collection_artists(
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    collection_artists = _services.get_collection_artists(db=db)
    return collection_artists

@app.post("/collection_artist_releases/{id_artist}")
def read_collection_artist_releases(
    id_artist: str,
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    db_collection_artist  = _services.get_collection_artist(db=db, id_artist=id_artist)
    if db_collection_artist is None:
        raise _fastapi.HTTPException(
            status_code=404, detail="Sorry, this artist doesn't exist in your collection"
        )
    collection_releases = _services.get_collection_artist_releases(db=db, id_artist=id_artist)
    return collection_releases

@app.post("/release_videos/{id_release}")
def read_release_videos(
    id_release: int,
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    release_videos = _services.get_release_videos(db=db, id_release=id_release)
    return release_videos

@app.post("/dendro_vertices/{id_hierarchy}")
def read_dendrogram_vertices(
    id_hierarchy: int,
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    dendrogram_vertices = _services.get_dendrogram_vertices(db=db, id_hierarchy=id_hierarchy)
    return dendrogram_vertices

@app.post("/dendro_edges/{id_hierarchy}")
def read_dendrogram_edges(
    id_hierarchy: int,
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    dendrogram_edges = _services.get_dendrogram_edges(db=db, id_hierarchy=id_hierarchy)
    return dendrogram_edges

@app.post("/spinder/")
def read_spinder(
    id_artist: Union[int, None] = None,
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    print(id_artist)
    if id_artist is None:
        spinder_suggestion = _services.get_spinder_random(db=db)
    else:
        spinder_suggestion = _services.get_spinder_artist(db=db, id_artist=id_artist)
    return spinder_suggestion


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
