from discogs.discogs import Discogs

discogs = Discogs(file_secrets="config/secrets.yml")
discogs.process_user_data()