# music_explorer_api

A suite of applications that aspires to enable you interacting with your collection as registered on Discogs.

The front end will be created by [Marcel Varkevisser](https://github.com/marcelvark) and can be found at ....

This repository provides an application to serve the data by allowing to query a music collection by exposing an API.

Components of this repository:

* A Python application that serves an API which allows exploration of the database.
* A Python application that scrapes data to create/fill the music collection database, this can be connected to your own Discogs account.
* Dockerfile that creates and image for the Python application
* An example of a docker-compose file
* A conf file for the [swag](https://github.com/linuxserver/docker-swag) reverse proxy

## A word from the author

This project is the result of another project where I tried a proof of concept in R [discogs_dashboard](https://github.com/mark-me/discogs_dashboard). The goal of this project was being inspired by my own music collection. I was wondering what kind of recommendations I could get from looking at the artists I have in my collection and their collaborations. Having an inkling how graph theory might be useful in this context, I started dabbling with the [igraph](https://igraph.org/) library. I was getting very promising results by using the [edge betweenness clustering](https://igraph.org/r/html/latest/cluster_edge_betweenness.html). Seeing my code was getting really messy and I wanted to increase my Python capabilities I chose to rewrite by using the lessons learned from my R project.

Now... If you are still interested and want to try this project out yourself, let me warn you in advance: this will need patience. Loading the data takes looooong since all is collected with Discogs API calls which aren't the fastest route to ingesting data.

## Spindler

This project is quite big in scope in terms of developing a UI, so we take a short detour by introducing a new app: [Spindler](https://marcel.website/spindler).

When I clean/reorder my collection I too often come across records of which I think: "GOD THIS IS GOOD! I NEED TO SPIN THIS!". And I mean _too_ often. I quickly get snowed under with those thoughts, while I need to finish the task at hand: reordering my collection.... What if I can create this experience of rediscovering my own collection with a kind of Tinder interface? Random suggestions and an option to get more of this or reject the choice and go in a totally different direction? OK.... I know this isn't how Tinder actually works, but it's the analogy that worked for me....

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

# Project board

[Kanban](https://github.com/users/mark-me/projects/1)

---

# Music Collection Toolkit for Discogs Users

Welcome to the **Music Collection Toolkit**, a suite of Python applications designed for music collectors who manage their collections on [Discogs.com](https://www.discogs.com/). This repository provides tools to help you explore, analyze, and interact with your collection data in new ways.

## Features

- **Data Loading**: Easily load and import your Discogs collection data into your preferred exploration environment.
- **API Service**: A powerful API to query and retrieve insights about your music collection. Get details like genres, formats, value estimates, and much more.
- **Graphical User Interface (GUI)**: An intuitive interface to browse and visualize your music collection, enabling you to filter by artist, genre, or format.

## Requirements

- Docker
- Docker Compose

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/music-collection-toolkit.git
   cd music-collection-toolkit
   ```

2. Set up your Discogs API key by following their [API documentation](https://www.discogs.com/developers/), and add your credentials to the `.env` file in the root of the project:
   ```
   DISCOGS_API_KEY=your_api_key
   ```

3. Build and start the services using Docker Compose:
   ```bash
   docker-compose up --build
   ```

   This will start all services, including the data loader, API service, and GUI.

## Usage

### Data Loading
Once the services are up, you can load your Discogs data into the environment using:
```bash
curl -X POST http://localhost:5000/load_data
```

### API Service
The API service will be available at `http://localhost:5000`. You can query your collection via the API, for example:
```bash
curl http://localhost:5000/collection/summary
```

### GUI Browser
Access the GUI by navigating to `http://localhost:8080` in your web browser. From there, you can browse and explore your music collection.

## Contributing

Contributions are welcome! Please submit pull requests or open an issue to discuss potential changes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

This version makes use of Docker Compose to streamline the installation and service management process.