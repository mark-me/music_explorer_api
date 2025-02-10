import os

from dotenv import dotenv_values
from fastapi import APIRouter, BackgroundTasks, HTTPException

from .etl import Discogs

config = {
    **dotenv_values(".env"),  # load shared development variables
    **os.environ,  # override loaded values with environment variables
}

discogs = Discogs(file_secrets="config/secrets.yml")

discogs_router = APIRouter(
    prefix='/discogs',
    tags=['Discogs resources']
)

@discogs_router.get("/check-credentials/")
async def check_user_credentials():
    """ Check if user credentials for Discogs are present
    """
    if discogs.check_user_tokens():
        return{'description': 'All OK!'}
    else:
        raise HTTPException(status_code=401, detail='Let user (re-)authorize access to her/his Discogs account')

@discogs_router.get("/get-user-access/")
async def open_discogs_permissions_page():
    """ Asks user to give app access to Discogs account, with a callback url to handle validation
    """
    callback_url = f"http://localhost:{config['PORT_LOADER']}/discogs/receive-token/"
    result = discogs.request_user_access(callback_url=callback_url)
    return result

@discogs_router.get("/receive-token/")
async def accept_user_token(oauth_token: str, oauth_verifier: str):
    """Callback function to process the user authentication result
    """
    result = discogs.save_user_token(oauth_verifier)
    return result

@discogs_router.get("/process_user_data/")
async def process_user_data(background_tasks: BackgroundTasks):
    background_tasks.add_task(discogs.process_user_data)
    return {"message": "Started processing user Discogs data"}
