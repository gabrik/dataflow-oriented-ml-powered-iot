FROM python:3.8-slim

RUN apt-get update --allow-insecure-repositories --allow-unauthenticated

RUN apt-get install -y curl

COPY ./requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY config.ini .
COPY ./dataset ./dataset
COPY ./script.sh ./script.sh
COPY ./house.py ./runnable.py

RUN chmod 755 script.sh