from typing import List

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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    