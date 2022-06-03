import datetime as _dt
import sqlalchemy as _sql
import sqlalchemy.orm as _orm

import database as _database

class CollectionArtist(_database.Base):
    __tablename__ = "vw_artists_qty_in_collection"

    id_artist = _sql.Column(_sql.String, primary_key=True)
    name_artist = _sql.Column(_sql.String)
    qty_collection_items = _sql.Column(_sql.Integer)
