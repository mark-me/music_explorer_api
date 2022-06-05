from typing import List
import datetime as _dt
import pydantic as _pydantic


class _CollectionArtistBase(_pydantic.BaseModel):
    id_artist: str
    name_artist: str
    url_artist_image: str
    qty_collection_items: str


class CollectionArtist(_CollectionArtistBase):
    pass


class _CollectionReleaseBase(_pydantic.BaseModel):
    id_artist: str
    name_artist: str
    name_release: str
    url_cover: str


class CollectionRelease(_CollectionReleaseBase):
    pass