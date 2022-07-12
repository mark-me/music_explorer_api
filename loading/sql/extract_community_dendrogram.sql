/* Create community vertices 
*/
CREATE TEMPORARY TABLE collection_ranked_eigenvalue AS
    SELECT id_community,
        name_artist,
        ROW_NUMBER() OVER (PARTITION BY id_community ORDER BY eigenvalue DESC) AS rank_eigenvalue
    FROM artist_community_hierarchy
    WHERE in_collection = 1;

CREATE TEMPORARY TABLE collection_community_label AS
    SELECT id_community, 
        GROUP_CONCAT(name_artist) AS label_community_collection
    FROM collection_ranked_eigenvalue
    WHERE rank_eigenvalue <= 3
    GROUP BY id_community;

CREATE TEMPORARY TABLE ranked_eigenvalue AS
    SELECT id_community,
        name_artist,
        ROW_NUMBER() OVER (PARTITION BY id_community ORDER BY eigenvalue DESC) AS rank_eigenvalue
    FROM artist_community_hierarchy;

CREATE TEMPORARY TABLE community_label AS
    SELECT id_community, 
        GROUP_CONCAT(name_artist) AS label_community
    FROM ranked_eigenvalue
    WHERE rank_eigenvalue <= 3
    GROUP BY id_community;

DROP TABLE IF EXISTS community_dendrogram_vertices;

CREATE TABLE community_dendrogram_vertices AS
    SELECT a.id_community,
        id_hierarchy + 1 AS id_hierarchy,
        label_community,
        label_community_collection,
        SUM(in_collection) AS qty_artists_collection,
        COUNT(*) as qty_artists
    FROM artist_community_hierarchy a
    LEFT JOIN community_label  b
        ON b.id_community = a.id_community
    LEFT JOIN collection_community_label c
        ON c.id_community = a.id_community
    GROUP BY a.id_community, id_hierarchy
    UNION
    SELECT 0 as id_community,
        0 AS id_hierarchy,
        label_community,
        label_community_collection,
        SUM(in_collection) AS qty_artists_collection,
        COUNT(*) as qty_artists
    FROM artist_community_hierarchy a
    LEFT JOIN community_label  b
        ON b.id_community = a.id_community
    LEFT JOIN collection_community_label c
        ON c.id_community = a.id_community        
    WHERE a.id_hierarchy = 0;

/* Create community edges 
*/
DROP TABLE IF EXISTS community_dendrogram_edges;

CREATE TABLE community_dendrogram_edges AS
    SELECT id_community_from as id_from, 
        id_community as id_to, id_hierarchy, 
        MAX(in_collection) AS to_collection_artists
    FROM artist_community_hierarchy
    GROUP BY id_community_from, 
        id_community, 
        id_hierarchy;
