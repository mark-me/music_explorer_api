from typing import List

import fastapi as _fastapi
import sqlalchemy.orm as _orm
import services as _services, schemas as _schemas

app = _fastapi.FastAPI()

_services.create_database()


@app.post("/collection_artists/", response_model=_schemas.CollectionArtist)
def read_collection_artists(
    skip: int=0, 
    limit: int=10, 
    db: _orm.Session=_fastapi.Depends(_services.get_db),
    ):
    collection_artists = _services.get_collection_artists(db=db, skip=skip, limit=limit)
    return collection_artists
