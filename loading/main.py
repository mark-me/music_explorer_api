import yaml
import extract as _extract


def main():
    with open(r'loading/config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    consumer_key = config['app_key']
    consumer_secret = config['app_secret']
    db_file = config['db_file']

    discogs_extractor = _extract.Discogs(consumer_key=consumer_key, consumer_secret=consumer_secret, db_file=db_file)
    discogs_extractor.start()
    db_extractor = _extract.Database(db_file=db_file)
    db_extractor.start()


if __name__ == "__main__":
    main()
