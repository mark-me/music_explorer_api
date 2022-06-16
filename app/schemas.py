from typing import List
import datetime as _dt
import pydantic as _pydantic


class CollectionArtist(_pydantic.BaseModel):
    id_artist: str
    name_artist: str
    url_artist_image: str
    qty_collection_items: str


class CollectionRelease(_pydantic.BaseModel):
    id_artist: str
    name_artist: str
    name_release: str
    url_cover: str


class ReleaseVideo(_pydantic.BaseModel):
    id_release: int
    title: str
    url_video: str
