from typing import List
import datetime as _dt
import pydantic as _pydantic


class _CollectionArtistBase(_pydantic.BaseModel):
    id_artist: str
    name_artist: str
    qty_collection_items: str


class CollectionArtist(_CollectionArtistBase):
    pass
