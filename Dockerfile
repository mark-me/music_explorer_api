FROM python:3.9

WORKDIR /api

COPY ./requirements.txt /api/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /api/requirements.txt

COPY ./api /code/api

CMD ["uvicorn", "api.main:api", "--host", "0.0.0.0", "--port", "80", "--proxy-headers"]
