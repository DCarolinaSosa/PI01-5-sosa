FROM python:3.9

WORKDIR /usr/src/app

RUN pip install mysql-connector-python pydantic uvicorn fastapi

COPY . /usr/src/app

EXPOSE 8000