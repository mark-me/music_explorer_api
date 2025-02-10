import os
from pathlib import Path

import uvicorn
from dotenv import dotenv_values
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from routers import discogs_etl, querying

config = {
    **dotenv_values(f"{Path(__file__).parent}/.env"),  # load shared development variables
    **os.environ,  # override loaded values with environment variables
}

app = FastAPI()
app.include_router(discogs_etl.router)
app.include_router(querying.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

@app.get("/", response_class=HTMLResponse)
async def welcome_page(request: Request):
    content = """
    <html>
        <head>
            <title>This is the API which can be used to grant access to your ...</title>
        </head>
        <body>
            <h1>The API for extracting Discogs information to a local database </h1>
            <p>Visit the <a href="/docs">Swagger UI</a> for the API documentation.</p>
        </body>
    </html>
    """
    return HTMLResponse(content=content)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5080)
