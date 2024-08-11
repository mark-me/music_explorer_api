from discogs.discogs import Discogs
import discogs.derive as extract

discogs = Discogs(file_secrets="app_loader/config/secrets.yml")

user = extract.User(discogs_client=discogs.client_discogs)

dict_user = user.get_data()
collection_item = user.get_link(type_link='connection_folder')
print("me")

