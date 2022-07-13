-- Is group
UPDATE artist
SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist);

-- Thumbnails
UPDATE artist
SET url_thumbnail = (
    SELECT url_thumbnail FROM (
        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases
        UNION
        SELECT id_member, url_thumbnail FROM artist_members
        UNION
        SELECT id_group, url_thumbnail FROM artist_groups
    ) WHERE id_artist = artist.id_artist );

-- Number of collection items
UPDATE artist
SET qty_collection_items = (SELECT COUNT(*)
                            FROM collection_items
                            INNER JOIN release_artists
                                ON release_artists.id_release = collection_items.id_release
                            WHERE  release_artists.id_artist = artist.id_artist);