import datetime as _dt
import sqlalchemy as _sql

import database as _database

class CollectionArtist(_database.Base):
    __tablename__ = "vw_artists_qty_in_collection"

    id_artist = _sql.Column(_sql.Integer, primary_key=True)
    name_artist = _sql.Column(_sql.String)
    url_image = _sql.Column(_sql.String)
    qty_collection_items = _sql.Column(_sql.Integer)


class CollectionRelease(_database.Base):
    __tablename__ = "vw_artist_collection_releases"

    id_artist = _sql.Column(_sql.Integer, primary_key=True)
    id_release = _sql.Column(_sql.Integer)
    name_artist = _sql.Column(_sql.String)
    name_release = _sql.Column(_sql.String, primary_key=True)
    url_cover = _sql.Column(_sql.String)


class ReleaseVideo(_database.Base):
    __tablename__ = "release_videos"

    id_release = _sql.Column(_sql.Integer, primary_key=True)
    title = _sql.Column(_sql.String)
    url_video = _sql.Column(_sql.String, primary_key=True)
