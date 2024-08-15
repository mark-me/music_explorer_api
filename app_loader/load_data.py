from discogs.discogs import Discogs

discogs = Discogs(file_secrets="app_loader/config/secrets.yml")
discogs.process_user_data()