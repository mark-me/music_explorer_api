import yaml
import db_utils as _db_utils
import extract as _extract


def main():
    with open(r'config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    consumer_key = config['app_key']
    consumer_secret = config['app_secret']
    db_file = config['db_file']

    db_manager = _db_utils.ManageDB(db_file=db_file)
    db_manager.create_backup()
    db_file = db_manager.create_load_copy()
    discogs_extractor = _extract.Discogs(consumer_key=consumer_key, consumer_secret=consumer_secret, db_file=db_file)
    discogs_extractor.start()
    db_extractor = _extract.Database(db_file=db_file)
    db_extractor.start()
    db_manager.replace_db()


if __name__ == "__main__":
    main()
