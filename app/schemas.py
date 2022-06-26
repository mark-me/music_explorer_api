from typing import List
import datetime as _dt
import pydantic as _pydantic


class CollectionArtist(_pydantic.BaseModel):
    id_artist: int
    name_artist: str
    url_artist_image: str
    qty_collection_items: int


class CollectionRelease(_pydantic.BaseModel):
    id_artist: int
    id_release: int
    name_artist: str
    name_release: str
    url_cover: str
    url_thumbnail: str


class ReleaseVideo(_pydantic.BaseModel):
    id_release: int
    title: str
    url_video: str
