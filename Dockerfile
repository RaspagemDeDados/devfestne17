FROM python:3.4

WORKDIR /app
ADD . /app

RUN apt update &&\
    apt -y upgrade &&\
    pip install -r /app/requirements.txt &&\
    python /app/setup.py install

ENTRYPOINT bash