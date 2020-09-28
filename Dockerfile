FROM python:3.7

WORKDIR /app

RUN pip install pipenv
COPY ./Pipfile /app/Pipfile
RUN pipenv install && pipenv shell

COPY ./imi /app

