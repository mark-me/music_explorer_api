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
    discogs_extractor.artists_collection()
    # discogs_extractor.artists_aliases()
    discogs_extractor.artists_members()
    discogs_extractor.artists_groups()


if __name__ == "__main__":
    main()
