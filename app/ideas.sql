
// Spider web for artist genres

SELECT artist.id_artist, artist.name_artist, name_genre, count(*)  * 100.0 / sum(count(*)) over(PARTITION BY artist.id_artist, artist.name_artist) AS perc
FROM artist
INNER JOIN release_artists
   ON release_artists.id_artist = artist.id_artist
INNER JOIN release_genres
	ON release_genres.id_release = release_artists.id_release
WHERE artist.id_artist IN ( 82294, 36665)
GROUP BY artist.id_artist, artist.name_artist, name_genre
ORDER BY artist.name_artist, COUNT(*) DESC