import requests
from requests.exceptions import HTTPError
import json
import numpy as np
import pandas as pd


class Discogs:

    def __init__(self, name_discogs_user, url_discogs_api) -> None:
        self.name_discogs_user = name_discogs_user
        self.url_discogs_api = url_discogs_api
        pass

    def items(self) -> pd.DataFrame:

        # Get first page to get the number of pages
        try:
            query = {'page': 1, 'per_page': 100}
            url_request = url_discogs_api + "/users/" + name_discogs_user + "/collection/folders/0/releases"
            response = requests.get(url_request, params=query)
            response.raise_for_status()
            jsonResponse = response.json()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        no_pages = jsonResponse["pagination"]["pages"] 

        # Retrieving all collection items
        collection_items = []
        for i in range(1, no_pages + 1):
            try:
                query = {'page': i, 'per_page': 100}
                url_request = url_discogs_api + "/users/" + name_discogs_user + "/collection/folders/0/releases"
                response = requests.get(url_request, params=query)
                jsonResponse = response.json()
                collection_items.append(pd.json_normalize(jsonResponse["releases"]))
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')
            except Exception as err:
                print(f'Other error occurred: {err}')

        df_collection = pd.concat(collection_items, ignore_index=True)
    
        return(df_collection)

    def artists(self, df_artists: pd.DataFrame) -> pd.DataFrame:
        pass