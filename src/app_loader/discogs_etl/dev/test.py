import yaml

import app_loader.discogs.db_utils as _db_utils
from discogs import Discogs

def main():
    with open(r'config.yml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    db_file = config['db_file']

    db_manager = _db_utils.ManageDB(db_file=db_file)
    # db_manager.create_backup()
    # db_file = db_manager.create_load_copy()
    discogs = Discogs(db_file=db_file)
    #if not discogs.check_user_tokens():

    #discogs_extractor.start()
    #db_manager.replace_db()


if __name__ == "__main__":
    main()