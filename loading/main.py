import datetime as dt
import time
import yaml

import extract as _extract
import store as _store

def main():

    with open(r'config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    url_discogs_api = 'https://api.discogs.com'
    name_discogs_user = config['discogs_user']
    token_discogs = config['discogs_token']
    db_file = config["db_file"]


    df_collection_items = _extract.collection_items(name_discogs_user, url_discogs_api)
    _store.collection_items(df_collection_items, db_file)



