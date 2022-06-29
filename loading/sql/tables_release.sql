CREATE TABLE "release" (
	"country"	TEXT,
	"url_thumbnail"	TEXT,
	"url_cover"	TEXT,
	"title"	TEXT,
	"id_release"	INTEGER,
	"date_released"	TEXT,
	"year"	INTEGER,
	"id_master"	INTEGER,
	"url_release"	TEXT,
	PRIMARY KEY("id_release")
)
CREATE INDEX idx_release_id_master ON release (id_master);


CREATE TABLE "release_artists" (
	"id_artist"	INTEGER,
	"id_release"	INTEGER,
	PRIMARY KEY("id_artist","id_release")
)

CREATE TABLE "release_credits" (
  "name_artist" TEXT,
  "role" TEXT,
  "id_artist" INTEGER,
  "api_artist" TEXT,
  "url_thumbnail" TEXT,
  "id_release" INTEGER
)
CREATE INDEX idx_release_credits_id_release ON release_credits (id_release);
CREATE INDEX idx_release_credits_id_artist ON release_credits (id_artist);

CREATE TABLE "release_formats" (
  "name_format" TEXT,
  "qty_format" TEXT,
  "id_release" INTEGER
)
CREATE INDEX idx_release_formats_id_release ON release_formats (id_release);

CREATE TABLE "release_genres" (
	"name_genre"	TEXT,
	"id_release"	INTEGER,
	PRIMARY KEY("id_release","name_genre")
)

CREATE TABLE "release_labels" (
  "name_label" TEXT,
  "catno" TEXT,
  "entity_type" TEXT,
  "entity_type_name" TEXT,
  "id_label" INTEGER,
  "resource_url" TEXT,
  "url_thumbnail" TEXT
)





