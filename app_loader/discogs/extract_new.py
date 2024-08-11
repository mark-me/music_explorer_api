"""Extraction process
    * Different methods for receiving data (list, atom)
    * Atom processing
        * Parse dictionary according to method (renaming columns, type casting)
    * Split dictionary to tables version based on method
    * Write to database
Returns:
    _type_: _description_
"""
from discogs_client import Client


class DiscogsFetcher:
    def __init__(self, client_discogs: Client) -> None:
        self.client_discogs = client_discogs

    def collection(self):
        me = self.client_discogs.identity()
        qty_items = me.collection_folders[0].count

class Extractor:
    def __init__(self, category: str, id: str, retrieval_func ,is_list: bool) -> None:
        self.category = category
        self.is_list = is_list
        self.extract_function(id)
        self.dict_rename = {}
        self.dict_typecast = {}

    def extract_function(self):
        if self.category == "CollectionItem":
            self.dict_rename = {}
            self.dict_typecast = {}
        elif self.category == "Artist":
            self.dict_rename = {}
            self.dict_typecast = {}

        # Rename dict items
        # Typecast dict items



class TransformItem:
    def __init__(self, dicts_renames: list, dicts_datatypes: list) -> None:
        self.dicts_renames = dicts_renames
        self.dicts_datatypes = dicts_datatypes

    def rename(self):
        if self.category == "CollectionItem":
            self.dict_rename = {}
        elif self.category == "Artist":
            self.dict_rename = {}

    def typecast(self):
        if self.category == "CollectionItem":
            self.dict_typecast = {}
        elif self.category == "Artist":
            self.dict_typecast = {}



class TransformList(TransformItem):
    def __init__(self, dicts_renames: list, dicts_datatypes: list) -> None:
        super().__init__(dicts_renames, dicts_datatypes)
