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
    SELECT artists.id_artist AS id_artist,
        artists.name_artist AS name_artist,
        collection_items.title as name_release,
        collection_items.url_cover as url_cover
    FROM artists
    INNER JOIN collection_artists
        ON collection_artists.id_artist = artists.id_artist
    INNER JOIN collection_items
        ON collection_items.id_release = collection_artists.id_release;

DROP VIEW IF EXISTS vw_alias_no_artist;

CREATE VIEW vw_alias_no_artist AS
    SELECT DISTINCT id_alias AS id_artist, api_alias as api_artist 
    FROM artist_aliases 
    WHERE id_alias NOT IN (SELECT id_artist FROM artist) AND
        api_alias IS NOT NULL AND LENGTH(api_alias) > 0;

DROP VIEW IF EXISTS vw_member_no_artist;

CREATE VIEW vw_member_no_artist AS
    SELECT DISTINCT id_member AS id_artist, api_member as api_artist 
    FROM artist_members 
    WHERE id_member NOT IN (SELECT id_artist FROM artist) AND
        api_member IS NOT NULL AND LENGTH(api_member) > 0;

DROP VIEW IF EXISTS vw_group_no_artist;

CREATE VIEW vw_group_no_artist AS
    SELECT DISTINCT id_group AS id_artist, api_group as api_artist 
    FROM artist_groups 
    WHERE id_group NOT IN (SELECT id_artist FROM artist) AND
        api_group IS NOT NULL AND LENGTH(api_group) > 0;

DROP VIEW IF EXISTS vw_artist_edges;

CREATE VIEW vw_artist_edges AS
    SELECT DISTINCT id_artist AS id_artist_from, 
            id_member         AS id_artist_to, 
            'group_member'    AS type_relation
    FROM artist_members
    UNION
    SELECT DISTINCT id_artist, 
            id_alias, 
            'artist_alias'
    FROM artist_aliases;
