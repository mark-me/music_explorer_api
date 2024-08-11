import datetime as dt
import time

import igraph as igraph
import pandas as pd
from tqdm import tqdm
from discogs_client import Client
from discogs_client.models import (
    PrimaryAPIObject,
    CollectionFolder,
    Artist as dg_Artist,
    CollectionItemInstance as gd_CollectionItemInstance,
)

class DiscogsItem:
    def __init__(self, discogs_client: Client) -> None:
        self.discogs_client = discogs_client
        self.dict_data = {}
        self.data_objects = {}  # table: list
        self.linked_items = {}  # discogs_items

    def get_data(self, type_object: str="self"):
        pass

    def get_link(self, type_link: str) -> PrimaryAPIObject:
        pass

    def extract_linked_items(self):
        pass

    def retry(self, func, ex_type=Exception, limit=0, wait_ms=100, wait_increase_ratio=2, logger=None):
        """
        Retry a function invocation until no exception occurs
        :param func: function to invoke
        :param ex_type: retry only if exception is subclass of this type
        :param limit: maximum number of invocation attempts
        :param wait_ms: initial wait time after each attempt in milliseconds.
        :param wait_increase_ratio: increase wait period by multiplying this value after each attempt.
        :param logger: if not None, retry attempts will be logged to this logging.logger
        :return: result of first successful invocation
        :raises: last invocation exception if attempts exhausted or exception is not an instance of ex_type
        """
        attempt = 1
        while True:
            try:
                return func()
            except Exception as ex:
                if not isinstance(ex, ex_type):
                    raise ex
                if 0 < limit <= attempt:
                    if logger:
                        logger.warning("no more attempts")
                    raise ex

                if logger:
                    logger.error("failed execution attempt #%d", attempt, exc_info=ex)

                attempt += 1
                if logger:
                    logger.info("waiting %d ms before attempt #%d", wait_ms, attempt)
                time.sleep(wait_ms / 1000)
                wait_ms *= wait_increase_ratio

class User(DiscogsItem):
    def __init__(self, discogs_client: Client) -> None:
        super().__init__(discogs_client)
        self.user = self.retry(self.discogs_client.identity())
        self.dict_data = self.user.data
        self.linked_items = self.extract_linked_items()

    def extract_linked_items(self):
        return {
            'collection_folder': self.user.collection_folders[0].releases,
        }

    def get_data(self) -> dict:
        return self.dict_data

    def get_link(self, type_link: str='collection_folder') -> PrimaryAPIObject:
        return self.linked_items['collection_folder']


class CollectionItems():
    def __init__(self, collection_folder: CollectionFolder) -> None:
        self.qty_items = collection_folder.count
        self.collection_list = collection_folder.releases
        self.collection_items = []


class Artist(DiscogsItem):
    def __init__(self, discogs_client: Client, item: PrimaryAPIObject) -> None:
        super().__init__(discogs_client, item)
