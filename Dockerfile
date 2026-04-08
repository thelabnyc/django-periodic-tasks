FROM registry.gitlab.com/thelabnyc/python:3.14@sha256:d0e7e3895e1e3c269cb56687664513a51dae0b86c22b1524c6d5332327bb8c6d
ENV PYTHONUNBUFFERED 1

RUN mkdir /code
WORKDIR /code

ADD . /code/
RUN uv sync
