FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
COPY ./config.yml /code/config.yml

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./loading /code/loading
COPY ./config.yml /code/loading/config.yml

COPY ./app /code/app
COPY ./config.yml /code/app/config.yml

WORKDIR /code/app

CMD [ "python", "./main.py"]