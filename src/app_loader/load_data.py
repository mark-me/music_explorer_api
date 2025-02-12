from app_loader.discogs_etl import Discogs

discogs = Discogs(file_secrets="config/secrets.yml", file_db="/data/music_collection.db")
discogs.process_user_data()