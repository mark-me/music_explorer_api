from typing import List

import uvicorn

import fastapi as _fastapi
import sqlalchemy.orm as _orm
import services as _services, schemas as _schemas

app = _fastapi.FastAPI()

_services.create_database()


@app.post("/collection_artists/")
def read_collection_artists(
    skip: int=0, 
    limit: int=999, 
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    collection_artists = _services.get_collection_artists(db=db, skip=skip, limit=limit)
    return collection_artists

@app.post("/artist_collection_releases/")
def read_artist_collection_releases(
    skip: int=0, 
    limit: int=999, 
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    collection_releases = _services.get_artist_collection_releases(db=db, skip=skip, limit=limit)
    return collection_releases




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    