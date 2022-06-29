CREATE TABLE "artist" (
	"name_artist"	TEXT,
	"id_artist"	INT,
	"role"	TEXT,
	"qty_collection_items"	INTEGER,
	PRIMARY KEY("id_artist")
);

CREATE TABLE "artist_aliases" (
	"id_alias"	INTEGER,
	"name_alias"	TEXT,
	"api_alias"	TEXT,
	"id_artist"	INTEGER,
	"url_thumbnail"	TEXT,
	PRIMARY KEY("id_artist", "id_alias")
);

CREATE TABLE "artist_members" (
	"id_member"	INTEGER,
	"name_member"	TEXT,
	"api_member"	TEXT,
	"is_active"	INTEGER,
	"id_artist"	INTEGER,
	"url_thumbnail"	TEXT,
	PRIMARY KEY("id_artist", "id_member")
)

CREATE TABLE "artist_groups" (
	"id_group"	INTEGER,
	"name_group"	TEXT,
	"api_group"	TEXT,
	"is_active"	INTEGER,
	"id_artist"	INTEGER,
	"url_thumbnail"	TEXT,
	PRIMARY KEY("id_group","id_artist")
);

CREATE TABLE artist_images (
    id_artist INTEGER, 
    type TEXT,                    
    url_image TEXT, 
    url_image_150 TEXT, 
    width_image INTEGER, 
    height_image INTEGER
);
CREATE INDEX idx_artist_images_id_artist ON artist_images (id_artist);

CREATE TABLE artist_masters (
    id_artist INTEGER, 
    id_master INTEGER, 
    title TEXT, 
    type TEXT, 
    id_main_release INTEGER,
    name_artist TEXT,
    role TEXT, 
    year INTEGER, 
    url_thumb TEXT
);
CREATE INDEX idx_artist_master_id_artist ON artist_masters (id_artist);
CREATE INDEX idx_artist_master_id_master ON artist_masters (id_master);

CREATE TABLE artist_urls (
    url_artist TEXT, 
    id_artist INTEGER
)
CREATE INDEX idx_artist_urls_id_artist ON artist_urls (id_artist);
