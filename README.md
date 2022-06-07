# music_explorer_api

This repository is part of a project where people can explore their musiccollection for using a webinterface.

The front end will be created by .... and can be found at ....

This repository provides an application to serve the data by allowing to query a music collection by exposing an API.

Components of this repository:
* A Python application that serves an API which allows exploration of the database
* A Python application that scrapes data to create/fill the music collection database
* Dockerfile that creates and image for the Python application
* A conf file for the [swag|https://github.com/linuxserver/docker-swag] reverse proxy


# Creating a virtual environment

## Create venv

```
pip3 install virtualenv
virtualenv music_explorer_api
source music_explorer_api/bin/activate
pip3 install -r requirements.txt
```

## Create requirements.txt

```
pip3 freeze > requirements.txt 
```

# Creating docker image

## Create

```
docker build -t ghcr.io/mark-me/musicexplorer:v0.0.1 .
```

## Push image to github

```
docker push ghcr.io/mark-me/musicexplorer:v0.0.1
```