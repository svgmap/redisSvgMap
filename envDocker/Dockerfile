FROM python:3.7-alpine

RUN apk upgrade --no-cache

RUN apk add gcc
RUN apk add musl-dev
RUN apk --update --no-cache add zlib-dev
RUN apk add jpeg-dev

RUN pip install --upgrade pip 
RUN pip install flask
RUN pip install redis
RUN pip install hiredis
RUN pip install numpy
RUN pip install pillow
