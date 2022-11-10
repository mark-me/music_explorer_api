from typing import List
import datetime as _dt
import pydantic as _pydantic


class CollectionArtist(_pydantic.BaseModel):
    id_artist: int
    name_artist: str
    url_artist_image: _pydantic.HttpUrl
    qty_collection_items: int


class CollectionRelease(_pydantic.BaseModel):
    id_artist: int
    id_release: int
    name_artist: str
    name_release: str
    url_cover: _pydantic.HttpUrl
    url_thumbnail: _pydantic.HttpUrl


class ReleaseVideo(_pydantic.BaseModel):
    id_release: int
    title: str
    url_video: _pydantic.HttpUrl


class DendrogramVertex(_pydantic.BaseModel):
    id_community: int
    id_hierarchy: int
    label_community: str
    label_community_collection: str
    qty_artists_collection: int
    qty_artists: int


class DendrogramEdge(_pydantic.BaseModel):
    id_from: int
    id_to: int
    id_hierarchy: int
    to_collection_artists: int


class Spinder(_pydantic.BaseModel):
    id_artist: int
    id_release: int
    name_artist: str
    name_release: str
    url_cover: _pydantic.HttpUrl
    url_thumbnail: _pydantic.HttpUrl
    id_artist_similar: int
    id_artist_dissimilar: int
    artists: List[CollectionArtist]