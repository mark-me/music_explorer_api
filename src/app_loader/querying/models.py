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
    id_release = _sql.Column(_sql.Integer, primary_key=True)
    name_artist = _sql.Column(_sql.String)
    name_release = _sql.Column(_sql.String)
    url_cover = _sql.Column(_sql.String)
    url_thumbnail = _sql.Column(_sql.String)


class ReleaseVideo(_database.Base):
    __tablename__ = "release_videos"

    id_release = _sql.Column(_sql.Integer, primary_key=True)
    title = _sql.Column(_sql.String)
    url_video = _sql.Column(_sql.String, primary_key=True)


class DendrogramVertices(_database.Base):
    __tablename__ = "community_dendrogram_vertices"

    id_community = _sql.Column(_sql.Integer, primary_key=True)
    id_hierarchy = _sql.Column(_sql.Integer)
    label_community = _sql.Column(_sql.String)
    label_community_collection = _sql.Column(_sql.String)
    qty_artists_collection = _sql.Column(_sql.Integer)
    qty_artists = _sql.Column(_sql.Integer)


class DendrogramEdges(_database.Base):
    __tablename__ = "community_dendrogram_edges"

    id_from = _sql.Column(_sql.Integer, primary_key=True)
    id_to = _sql.Column(_sql.Integer, primary_key=True)
    id_hierarchy = _sql.Column(_sql.Integer)
    to_collection_artists = _sql.Column(_sql.Integer)


class Spinder(_database.Base):
    __tablename__ = "vw_spinder_random"

    id_artist = _sql.Column(_sql.Integer, primary_key=True)
    name_artist = _sql.Column(_sql.String)
    id_artist_similar = _sql.Column(_sql.Integer)
    id_artist_dissimilar = _sql.Column(_sql.Integer)
    id_release = _sql.Column(_sql.Integer, primary_key=True)
    name_release = _sql.Column(_sql.String)
    url_cover = _sql.Column(_sql.String)
    url_thumbnail = _sql.Column(_sql.String)

class SpinderArtist(_database.Base):
    __tablename__ = "vw_spinder_artist"

    id_artist = _sql.Column(_sql.Integer, primary_key=True)
    name_artist = _sql.Column(_sql.String)
    id_artist_similar = _sql.Column(_sql.Integer)
    id_artist_dissimilar = _sql.Column(_sql.Integer)
    id_release = _sql.Column(_sql.Integer, primary_key=True)
    name_release = _sql.Column(_sql.String)
    url_cover = _sql.Column(_sql.String)
    url_thumbnail = _sql.Column(_sql.String)