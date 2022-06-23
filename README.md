# music_explorer_api

This repository is part of a project where people can explore their music collection for using a web-interface.

The front end will be created by [Marcel Varkevisser](https://github.com/marcelvark) and can be found at ....

This repository provides an application to serve the data by allowing to query a music collection by exposing an API.

Components of this repository:
* A Python application that serves an API which allows exploration of the database
* A Python application that scrapes data to create/fill the music collection database, this can be connected to your own Discogs account.
* Dockerfile that creates and image for the Python application
* An example of a docker-compose file
* A conf file for the [swag](https://github.com/linuxserver/docker-swag) reverse proxy

# A word from the author

This project is the result of another project where I tried a proof of concept in R [discogs_dashboard](https://github.com/mark-me/discogs_dashboard). The goal of this project was being inspired by my own music collection. I was wondering what kind of recommendations I could get from looking at the artists I have in my collection and their collaborations. Having an inkling how graph theory might be useful in this context, I started dabbling with the [igraph](https://igraph.org/) library. I was getting very promising results by using the [edge betweenness clustering](https://igraph.org/r/html/latest/cluster_edge_betweenness.html). Seeing my code was getting really messy and I wanted to learn python I chose to rewrite and use the results of my findings.

Now... If you are still interested and want to try this project out yourself, let me warn you in advance: this will need patience. Loading the data takes looooong since all is collected with API calls that aren't the fastest way for ingesting data. Next to that the clustering takes quite some computing power, which at the time of this writing also means: more time.....

# Snippets

## Creating a virtual environment

First install the virtual environment package:
```
pip3 install virtualenv
```
Create a virtual environment
```
virtualenv music_explorer_api
```

Activate virtual environment
```
source music_explorer_api/bin/activate
pip3 install -r requirements.txt
```

install package
```
pip3 install 
```

### Create requirements.txt

```
pip3 freeze > requirements.txt 
```

## Creating docker image

### Create

```
docker build -t ghcr.io/mark-me/musicexplorer:v0.0.1 .
```

### Push image to github

```
docker push ghcr.io/mark-me/musicexplorer:v0.0.1
```
