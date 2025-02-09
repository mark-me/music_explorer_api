FROM python:3.12.3-slim-bookworm

WORKDIR /app
RUN mkdir config
RUN mkdir artist_images
COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .
RUN rm .env

VOLUME /data
VOLUME /data/artist_images

EXPOSE 5001

CMD ["./entrypoint.sh"]