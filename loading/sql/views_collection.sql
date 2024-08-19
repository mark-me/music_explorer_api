CREATE VIEW vw_artist_collection_releases AS
    SELECT DISTINCT release_artists.id_artist,
        collection_items.id_release,
        name_artist,
        release.title AS name_release,
        release.url_cover,
        release.url_thumbnail
    FROM collection_items
    INNER JOIN release_artists
        ON release_artists.id_release = collection_items.id_release
    INNER JOIN artist
        ON artist.id_artist = release_artists.id_artist
    INNER JOIN release
        ON release.id_release = release_artists.id_release;

CREATE VIEW vw_artists_qty_in_collection AS
    SELECT artist.id_artist,
        name_artist,
        artist_images.url_image AS url_image,
        COUNT(*) AS qty_collection_items
    FROM artist
    LEFT JOIN artist_images
        ON artist_images.id_artist = artist.id_artist
    INNER JOIN release_artists
        ON artist.id_artist = release_artists.id_artist
    INNER JOIN release
        ON release.id_release = release_artists.id_release
    INNER JOIN collection_items
        ON collection_items.id_release = release.id_release
    WHERE artist_images.type = 'primary' OR artist_images.type IS NULL
    GROUP BY artist.id_artist, name_artist, artist_images.url_image
    ORDER BY COUNT(*) DESC;

CREATE VIEW vw_artist_vertices AS
    SELECT
        id_artist,
        MAX(in_collection) AS in_collection
    FROM (  SELECT id_artist, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist
                UNION
            SELECT id_alias, 0 as in_collection from artist_aliases
                UNION
            SELECT id_member, 0 FROM artist_members
                UNION
            SELECT id_group, 0 FROM artist_groups
                UNION
            SELECT id_artist, 0 FROM artist_masters WHERE role IN ('Main', 'Appearance', 'TrackAppearance')
                UNION
            SELECT release_artists.id_artist, MAX(IIF(date_added IS NULL, 0, 1))
            FROM release_artists
            INNER JOIN release
                ON release.id_release = release_artists.id_release
            LEFT JOIN collection_items
                ON collection_items.id_release = release.id_release
            GROUP BY release_artists.id_artist )
    GROUP BY id_artist

CREATE VIEW vw_artist_edges AS
    SELECT DISTINCT
        id_artist_from,
        id_artist_to,
        relation_type
    FROM (  SELECT id_member AS id_artist_from, id_artist as id_artist_to, 'group_member' as relation_type
            FROM artist_members
        UNION
            SELECT id_artist, id_group, 'group_member' FROM artist_groups
        UNION
            SELECT a.id_alias, a.id_artist, 'artist_alias'
            FROM artist_aliases a
            LEFT JOIN artist_aliases b
                ON a.id_artist = b.id_alias AND
                    a.id_alias = b.id_artist
            WHERE a.id_artist > b.id_artist OR b.id_artist IS NULL
        UNION
            SELECT a.id_artist, b.id_artist, 'co_appearance'
            FROM release_artists a
            INNER JOIN release_artists b
                ON b.id_release = a.id_release
            WHERE a.id_artist != b.id_artist )
    GROUP BY id_artist_from, id_artist_to, relation_type;