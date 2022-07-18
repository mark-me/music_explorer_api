-- Is group
CREATE INDEX IF NOT EXISTS idx_artist_members_id_artist ON artist_members (id_artist);

UPDATE artist
SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist);

-- Thumbnails

CREATE TEMPORARY TABLE thumbnails AS
        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases
        UNION
        SELECT id_member, url_thumbnail FROM artist_members
        UNION
        SELECT id_group, url_thumbnail FROM artist_groups;

UPDATE artist
SET url_thumbnail = ( SELECT thumbnails WHERE id_artist = artist.id_artist );

-- Number of collection items
CREATE TEMPORARY TABLE qty_collection_items AS
    SELECT release_artists.id_artist AS id_artist, COUNT(*) AS qty_items
    FROM collection_items
    INNER JOIN release_artists
        ON release_artists.id_release = collection_items.id_release
    GROUP BY release_artists.id_artist;

UPDATE artist
SET qty_collection_items = (SELECT qty_items
                            FROM qty_collection_items
                            WHERE  qty_collection_items.id_artist = artist.id_artist);