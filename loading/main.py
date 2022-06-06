import datetime as dt
import time
import yaml

import extract as _extract
import derive as _derive
import store as _store

def main():

    with open(r'loading/config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    url_discogs_api = 'https://api.discogs.com'
    name_discogs_user = config['discogs_user']
    token_discogs = config['discogs_token']
    db_file = config["db_file"]

    discogs_extractor = _extract.Discogs(name_discogs_user=name_discogs_user, discogs_token=token_discogs, url_discogs_api=url_discogs_api)
    
    # Load collection item data
    collection_store = _store.Collection(db_file=db_file)
    df_collection_items = discogs_extractor.collection_items()
    collection_store.write_items(df_collection_items)
    
    collection_derive = _derive.Collection(df_releases=df_collection_items)
    collection_store.write_artists(collection_derive.artists())
    collection_store.write_formats(collection_derive.formats())
    collection_store.write_labels(collection_derive.labels())
    collection_store.write_genres(collection_derive.genres())
    collection_store.write_styles(collection_derive.styles())

    del df_collection_items

    # Load artist data
    artist_store = _store.Artists(db_file=db_file)
    df_artist_new = collection_store.read_new_artist_id()
    df_artist_new = discogs_extractor.artists(df_artists=df_artist_new)
    artist_store.write_artists(df_artists=df_artist_new)

    
if __name__ == "__main__":
    main()
