CREATE INDEX IF NOT EXISTS community_dendrogram_vertices_id_community ON community_dendrogram_vertices (id_community);

DROP VIEW IF EXISTS vw_spinder_random;

DROP VIEW IF EXISTS vw_spinder_artist;

-- Selecting dissimilar
-- Putting artists in their own split from the main pool (first split in the dendrogram)
CREATE TEMPORARY TABLE artist_cluster_branch AS
SELECT id_artist,
    name_artist,
    MAX(id_hierarchy) AS id_hierarchy,
    MIN(id_community) AS id_community_min,
    MAX(id_community) AS id_community_max
FROM artist_community_hierarchy
WHERE id_community > 1 AND
    in_collection = 1
GROUP BY id_artist,
    name_artist;

-- Creating all combinations of artists that are in different branches
DROP TABLE IF EXISTS artist_dissimilar;

CREATE TABLE artist_dissimilar AS
SELECT a.id_artist,
    b.id_artist as id_artist_dissimilar
FROM artist_cluster_branch a
CROSS JOIN artist_cluster_branch    b
WHERE b.id_artist != a.id_artist AND
    b.id_community_min != a.id_community_min AND
    b.id_community_max != a.id_community_max;

CREATE INDEX IF NOT EXISTS artist_dissimilar_id_artist ON artist_dissimilar (id_artist);

-- Select similar
-- Select the artists most specific cluster where there is at least one other artist in the collection
CREATE TEMPORARY TABLE artist_alternative_cluster AS
SELECT a.id_artist,
    a.name_artist,
    MAX(a.id_hierarchy) AS id_hierarchy,
    MAX(a.id_community) AS id_community
FROM artist_community_hierarchy  a
INNER JOIN community_dendrogram_vertices   b
    ON b.id_community = a.id_community
WHERE a.id_community > 1 AND
    b.qty_artists_collection > 1 AND
    a.in_collection = 1
GROUP BY a.id_artist, a.name_artist;

DROP TABLE IF EXISTS artist_similar;

CREATE TABLE artist_similar AS
SELECT a.id_artist,
    b.id_artist as id_artist_similar,
    a.id_community
FROM artist_alternative_cluster  a
INNER JOIN artist_community_hierarchy   b
    ON b.id_community = a.id_community
WHERE a.id_artist != b.id_artist AND
    b.in_collection = 1;

CREATE INDEX IF NOT EXISTS artist_similar_id_artist ON artist_similar (id_artist);

CREATE VIEW vw_spinder_random AS
SELECT a.id_artist,
    a.name_artist,
    s.id_community,
    s.id_artist_similar AS id_artist_similar,
    d.id_artist_dissimilar AS id_artist_dissimilar,
    r.id_release,
    r.title AS name_release,
    r.url_cover,
    r.url_thumbnail
FROM artist a
LEFT JOIN artist_similar   s
    ON s.id_artist = a.id_artist
INNER JOIN artist_dissimilar    d
    ON d.id_artist = a.id_artist
INNER JOIN release_artists  ra
    ON ra.id_artist = a.id_artist
INNER JOIN collection_items  c
    ON c.id_release = ra.id_release
INNER JOIN release  r
    ON r.id_release = ra.id_release
WHERE a.id_artist IN (SELECT id_artist
                    FROM artist
                    WHERE qty_collection_items > 0
                    ORDER BY RANDOM()
                    LIMIT 1)
ORDER BY RANDOM()
LIMIT 1;

CREATE VIEW vw_spinder_artist AS
SELECT a.id_artist,
    a.name_artist,
    s.id_artist_similar AS id_artist_similar,
    d.id_artist_dissimilar AS id_artist_dissimilar,
    r.id_release,
    r.title AS name_release,
    r.url_cover,
    r.url_thumbnail
FROM artist a
LEFT JOIN artist_similar   s
    ON s.id_artist = a.id_artist
INNER JOIN artist_dissimilar    d
    ON d.id_artist = a.id_artist
INNER JOIN release_artists  ra
    ON ra.id_artist = a.id_artist
INNER JOIN collection_items  c
    ON c.id_release = ra.id_release
INNER JOIN release  r
    ON r.id_release = ra.id_release
ORDER BY RANDOM();