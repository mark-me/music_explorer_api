# music_explorer_api

This repository is part of a project where people can explore their music collection using a web-interface.

The front end will be created by [Marcel Varkevisser](https://github.com/marcelvark) and can be found at ....

This repository provides an application to serve the data by allowing to query a music collection by exposing an API.

Components of this repository:
* A Python application that serves an API which allows exploration of the database
* A Python application that scrapes data to create/fill the music collection database, this can be connected to your own Discogs account.
* Dockerfile that creates and image for the Python application
* An example of a docker-compose file
* A conf file for the [swag](https://github.com/linuxserver/docker-swag) reverse proxy

# A word from the author

This project is the result of another project where I tried a proof of concept in R [discogs_dashboard](https://github.com/mark-me/discogs_dashboard). The goal of this project was being inspired by my own music collection. I was wondering what kind of recommendations I could get from looking at the artists I have in my collection and their collaborations. Having an inkling how graph theory might be useful in this context, I started dabbling with the [igraph](https://igraph.org/) library. I was getting very promising results by using the [edge betweenness clustering](https://igraph.org/r/html/latest/cluster_edge_betweenness.html). Seeing my code was getting really messy and I wanted to increase my Python capabilities I chose to rewrite by using the lessons learned from my R project.

Now... If you are still interested and want to try this project out yourself, let me warn you in advance: this will need patience. Loading the data takes looooong since all is collected with Discogs API calls which aren't the fastest way for ingesting data.

## Spinder

This project is quite big in scope in terms of developing a UI, so we take a short detour by introducing a new app: Spinder.

When I clean/reorder my collection I too often come across records of which I think: GOD THIS IS GOOD: I NEED TO SPIN THIS! And I mean _too_ often. I quickly get snowed under with those thoughts, while I need to finish the task at hand: reordering my collection.... What if I can create this rediscovery of my own collection with a kind of Tinder interface? Random suggestion and an option to get more of this or reject the choice and go in a totally different direction? OK.... I know this isn't how Tinder actually works, but the analogy worked for me....

So I am introducing a new API endpoint to support this kind of functionality. It will process a request as follows:
* Get random artist or requested artist
* Select random collection item
* Select random most similar artist id from most specific cluster
* Select random dissimilar artist id (from other cluster dendrogram branch)

The API endpoint will return data with the following information:
* An artist
* A random release of the artist
* A random artiest from the same niche
* A random artiest a totally different niche

## Extra connections

There are loads of singers that cover the same songs, which could also be considered as some bond....

```
CREATE TEMPORARY TABLE multi_tracks AS
SELECT title
FROM release_tracks
INNER JOIN release_track_artists
    ON release_track_artists.id_release = release_tracks.id_release
GROUP BY title
HAVING COUNT(DISTINCT id_artist) > 1;

SELECT DISTINCT multi_tracks.title,
    id_artist,
    name_artist
FROM multi_tracks
INNER JOIN release_tracks
    ON release_tracks.title = multi_tracks.title
INNER JOIN release_track_artists
    ON release_track_artists.id_release = release_tracks.id_release AND
        release_track_artists.position = release_tracks.position
ORDER BY multi_tracks.title;
```

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

# Notes
**2022-11-04**
Additions to point 07 'Artist in suggestion list' from our last meeting:
- A list with artists which are from the 'similarity neighborhood' (if similarity neighborhood make sense to you, I don't know). Each of which has a:
  - head
  - name
  - number for the albums in collection
  - nearest connecting artist name (and head?) (I have some thoughts on this, but this is better explained in person)
- Clicking one of these list items results in a random record of this artist

**2022-10-29**
- 01 - V: Add an SVG button for adding suggestions to a localStorage (parkeer) list.
- 02 - M: Send suggestion lists (all suggestions at once).
- 03 - V: Walk through the suggestion lists front-end.
- 04 - V: Hover overlay with icons for click actions: also fadeout on refresh.
- 05 - V: Propose ideas for similar suggestions.
- 06 - V: Think about the UI: the suggestions are artists, for which albums are randomly chosen, and presented as suggestions.
- 07 - V: Think about viewing/navigating to:
  - Artists in suggestion list (head + name)
  - Other albums of artists
  - Parking list
- 08 - M: Think about suggestion similarity measure, for "user control" (V: settings).
