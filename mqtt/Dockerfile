FROM python:3.8-slim

RUN apt-get update --allow-insecure-repositories --allow-unauthenticated

RUN apt-get install -y curl

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt