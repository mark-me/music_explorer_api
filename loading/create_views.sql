DROP VIEW IF EXISTS vw_artists_qty_in_collection;

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


DROP VIEW IF EXISTS vw_artist_collection_releases;

CREATE VIEW vw_artist_collection_releases AS
    SELECT DISTINCT release_artists.id_artist, 
        name_artist, 
        release.title, 
        release.url_cover
    FROM collection_items
    INNER JOIN release_artists
        ON release_artists.id_release = collection_items.id_release
    INNER JOIN artist
        ON artist.id_artist = release_artists.id_artist
    INNER JOIN release
        ON release.id_release = release_artists.id_release;


DROP VIEW IF EXISTS vw_artists_not_added;

CREATE VIEW vw_artists_not_added AS
    SELECT DISTINCT id_artist
    FROM (
            SELECT id_artist
            FROM release_artists
            UNION
            SELECT id_alias
            FROM artist_aliases
            UNION
            SELECT id_member
            FROM artist_members
            UNION 
            SELECT id_group
            FROM artist_groups
        )
    WHERE id_artist NOT IN ( SELECT id_artist FROM artist );
