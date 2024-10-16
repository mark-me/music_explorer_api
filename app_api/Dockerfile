FROM python:3.12.3-slim-bookworm

RUN apt-get update -y
RUN apt-get install -y pkg-config \
    libcairo2-dev \
    libjpeg-dev \
    libgif-dev \
    gcc

WORKDIR /app
RUN mkdir config
RUN mkdir artist_images
COPY ../requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .
#RUN rm .env

VOLUME /app/config
VOLUME /app/artist_images

EXPOSE 5000

CMD ["./entrypoint.sh"]