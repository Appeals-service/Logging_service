FROM python:3.11.7-alpine

ENV POETRY_VERSION=1.7.1

WORKDIR /opt/src/

RUN apk update
RUN pip install --upgrade pip

COPY poetry.lock pyproject.toml ./

RUN apk add --no-cache gcc build-base libffi-dev musl-dev postgresql-dev

RUN yes | pip install --no-cache-dir "poetry==$POETRY_VERSION" && poetry config virtualenvs.create false && poetry install --only main --no-interaction --no-ansi


#RUN #python -m pip install --upgrade pip && \
#    pip install poetry --no-cache-dir "poetry==$POETRY_VERSION" && \
#    poetry config virtualenvs.create false && \
#    poetry install --no-dev --no-root --no-cache --no-interaction

COPY src .
