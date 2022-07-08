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