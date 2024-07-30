import logging
import os
from pathlib import Path
import time

import discogs_client
from discogs_client.exceptions import HTTPError

from db_utils import ManageDB
import extract as _extract
from utils import SecretsYAML

logging.basicConfig(
    format="%(levelname)s:\t%(asctime)s - %(module)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class Discogs:
    def __init__(self) -> None:
        self._consumer_key = "zvHFpFQWJrdDfCwoLalG"
        self._consumer_secret = "FzRxDEGBbvWZpAmkQKBYHYeNdIjKxnVO"
        self._secrets = {"name": None, "secret": None, "token": None, "user": None}
        self._user_secrets_file = SecretsYAML(
            file_path="config/secrets.yml",
            app="discogs",
            expected_keys=set(self._secrets.keys()),
        )
        self.user_agent = "boelmuziek"
        self.discogsclient = discogs_client.Client(
            self.user_agent,
            consumer_key=self._consumer_key,
            consumer_secret=self._consumer_secret,
        )
        self.check_user_tokens()
        self.set_path_artist_images()

    def set_path_artist_images(self):
        self._path_artist_cache = "artist_images"
        # Create artist image cache directory if not exists
        if not os.path.exists(self._path_artist_cache):
            os.makedirs(self._path_artist_cache)

    @property
    def user_token(self):
        return self._user_token

    @property
    def user_secret(self):
        return self._user_secret

    def check_user_tokens(self) -> dict:
        result = self._user_secrets_file.read_secrets()
        if result is not None:
            logger.info("Found user token in config file config/secrets.yml")
            self.discogsclient.set_token(token=result["token"], secret=result["secret"])
            return True
        else:
            logger.warning(
                "No user token found, user needs to authenticate the app use on Discogs"
            )
            return False

    def request_user_access(self, callback_url: str = None) -> dict:
        """Prompt your user to "accept" the terms of your application. The application
        will act on behalf of their discogs.com account."""
        logger.info(
            f"Requesting user access to Discogs account with callback {callback_url}"
        )
        self._user_token, self._user_secret, url = self.discogsclient.get_authorize_url(
            callback_url=callback_url
        )
        return {
            "message": "Authorize BoelMuziek for access to your Discogs account :",
            "url": url,
        }

    def save_user_token(self, verification_code: str) -> dict:
        """If the user accepts, discogs displays a key to the user that is used for
        verification. The key is required in the 2nd phase of authentication."""
        oauth_verifier = verification_code
        try:
            logger.info(
                "Receiving confirmation of access to the user's Discogs account"
            )
            self._user_token, self._user_secret = self.discogsclient.get_access_token(
                oauth_verifier
            )
        except HTTPError:
            logger.error("Failed to authenticate.")
            return {"status_code": 401, "detail": "Unable to authenticate."}
        user = (
            self.discogsclient.identity()
        )  # Fetch the identity object for the current logged in user.
        # Write to secrets file
        dict_user = {
            "token": self._user_token,
            "secret": self._user_secret,
            "user": user.username,
            "name": user.name,
        }
        self._user_secrets_file.write_secrets(dict_secrets=dict_user)
        logger.info(f"Connected and written user credentials of {user.name}.")
        return {"status_code": 200, "message": f"User {user.username} connected."}

    def process_user_data(self):
        db_file = "/data/music_collection.db"

        db_manager = ManageDB(db_file=db_file)
        db_manager.create_backup()
        db_file = db_manager.create_load_copy()
        discogs_extractor = _extract.Discogs(
            consumer_key=self._consumer_key,
            consumer_secret=self._consumer_secret,
            db_file=db_file,
        )
        discogs_extractor.start()
        db_manager.replace_db()

    def _save_artist_image_cache(self, name_artist: str, image: bytes):
        """Save an image to the cache directory

        Args:
            name_artist (str): Name of the artist
            image (bytes): Artist image binary
        """
        path_image = f"{self._path_artist_cache}/{name_artist}.jpg"
        with open(path_image, "wb") as file:
            file.write(image)
        logger.info(f"Artist image saved for: {name_artist}")

    def _get_artist_image_cached(self, name_artist: str) -> bytes:
        """Reading an cached artist image

        Args:
            name_artist (str): Name of the artist

        Returns:
            bytes: Artist image
        """
        path_image = f"{self._path_artist_cache}/{name_artist}.jpg"  # File image cache
        if Path(path_image).is_file():
            logger.info(f"Loading art of {name_artist} from cache.")
            with open(path_image, "rb") as file:
                image = file.read()
            return image
        else:
            return None

    def get_artist_image(self, name_artist: str) -> dict:
        """Retrieve an artist image

        Args:
            name_artist (str): Name of the artist

        Returns:
            dict: Dictionary with the retrieval status and image (or error message)
        """
        image = self._get_artist_image_cached(name_artist=name_artist)
        if image is not None:
            return {"status_code": 200, "message": image}

        # Fetch image from discogs
        artist_data = self.get_artist(name_artist=name_artist)
        if artist_data is not None:
            try:
                image_url = artist_data["images"][0]["uri"]
                image, resp = self.discogsclient._fetcher.fetch(
                    None, "GET", image_url, headers={"User-agent": self.user_agent}
                )
                # Write artist image to cache
                self._save_artist_image_cache(name_artist=name_artist, image=image)
            except (TypeError, IndexError):
                msg = f"No artist image found for: {name_artist}"
                logger.error(msg)
                return {"status_code": 404, "detail": msg}
            except HTTPError as e:
                if e.status_code == 429:
                    time.sleep(60)
            return {"status_code": resp, "message": image}
        else:
            return {"status_code": 404, "detail": msg}

    def artist_images_bulk(self, list_artists: list) -> None:
        for artist in list_artists:
            self.get_artist_image(name_artist=artist.artist)
