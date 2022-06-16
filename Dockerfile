FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./loading /code/loading

COPY ./app /code/app

WORKDIR /code/app

CMD [ "python", "./main.py"]