import datetime as dt
import time
import yaml

import extract as _extract
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


if __name__ == "__main__":
    main()
