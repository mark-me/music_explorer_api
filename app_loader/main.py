import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import uvicorn
from dotenv import dotenv_values

from routers import (
    discogs,
)

file_dot_env = os.path.dirname(os.path.abspath(__file__)) + "/.env"
config = {
    **dotenv_values(file_dot_env),  # load shared development variables
    **os.environ,  # override loaded values with environment variables
}

app = FastAPI()
app.include_router(discogs.router)


@app.get("/", response_class=HTMLResponse)
async def welcome_page(request: Request):
    content = """
    <html>
        <head>
            <title>The controller welcomes you...</title>
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
