FROM python:3.9.18-slim-bookworm

WORKDIR /app
COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY . .
RUN rm .env

EXPOSE 5002

CMD ["./entrypoint.sh"]