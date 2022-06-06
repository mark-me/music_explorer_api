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

    discogs_extractor = _extract.Discogs(name_discogs_user=name_discogs_user, url_discogs_api=url_discogs_api)
    
    collection_store = _store.Collection(db_file=db_file)
    df_collection_items = discogs_extractor.collection_items()
    collection_store.items(df_collection_items)
    
    collection_derive = _derive.Collection(df_releases=df_collection_items)
    collection_store.artists(collection_derive.artists())
    collection_store.formats(collection_derive.formats())
    collection_store.labels(collection_derive.labels())
    collection_store.genres(collection_derive.genres())
    collection_store.styles(collection_derive.styles())

    print(df_collection_items.info(memory_usage="deep"))

if __name__ == "__main__":
    main()
