import datetime as dt
from difflib import diff_bytes
import time
import yaml

import extract as _extract
import derive as _derive
import db_writer as _db_writer
import db_reader as _db_reader

def main():

    with open(r'loading/config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    name_discogs_user = config['discogs_user']
    token_discogs = config['discogs_token']
    db_file = config["db_file"]

    discogs_extractor = _extract.Discogs(name_discogs_user=name_discogs_user, discogs_token=token_discogs, db_file=db_file)
    discogs_extractor.collection_items()
    discogs_extractor.artists()
    

def new_collection_artists(discogs_extractor: _extract.Discogs, db_file: str):
    # Load artists that haven't previously been stored
    collection_db = _db_reader.Collection(db_file=db_file)
    artist_store = _db_writer.Artists(db_file=db_file)
    df_artist_new = collection_db.new_artists()
    if(df_artist_new.shape[0] > 0):
        df_artist_new = discogs_extractor.artists(df_artists=df_artist_new)
        artist_store.artists(df_artists=df_artist_new)
        artist_derive = _derive.Artists(df_artist=df_artist_new)
        artist_store.images(df_images=artist_derive.images())
        artist_store.urls(df_urls=artist_derive.urls())
        artist_store.aliases(df_aliases=artist_derive.aliases())
        artist_store.groups(df_groups=artist_derive.groups())
        artist_store.members(df_members=artist_derive.members()) 

if __name__ == "__main__":
    main()
