DROP TABLE IF EXISTS artist_relations;

CREATE TABLE artist_relations AS
SELECT DISTINCT id_artist_from,
                id_artist_to,
                relation_type
FROM (  SELECT id_member AS id_artist_from,
            id_artist as id_artist_to,
            'group_member' as relation_type
        FROM artist_members
    UNION
        SELECT id_artist as id_artist_from,
            id_group AS id_artist_to,
            'group_member' as relation_type
        FROM artist_groups
    UNION
        SELECT a.id_alias,
            a.id_artist,
            'artist_alias'
        FROM artist_aliases a
        LEFT JOIN artist_aliases b
            ON a.id_artist = b.id_alias AND
                a.id_alias = b.id_artist
        WHERE a.id_artist > b.id_artist OR
            b.id_artist IS NULL
    UNION
        SELECT a.id_artist,
            b.id_artist,
            'co_appearance'
        FROM release_artists a
        INNER JOIN release_artists b
            ON b.id_release = a.id_release
        WHERE a.id_artist != b.id_artist
    UNION
        SELECT a.id_artist,
            b.id_artist,
            'release_role'
        FROM release_artists a
        INNER JOIN release_credits b
            ON b.id_release = a.id_release
        INNER JOIN role c
            ON c.role = b.role
        WHERE a.id_artist = b.id_artist AND
            as_edges = 1
    )
INNER JOIN artist a
    ON a.id_artist = id_artist_from
INNER JOIN artist b
    ON b.id_artist = id_artist_to
GROUP BY id_artist_from,
    id_artist_to,
    relation_type;