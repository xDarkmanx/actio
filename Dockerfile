FROM python:3.12-bookworm AS py-base
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1

WORKDIR /app

RUN apt update && \
    apt dist-upgrade -y && \
    apt install -y \
        python3-poetry

COPY src/actio /app/src/actio
COPY pyproject.toml /app/
COPY README.md /app/
# COPY examples/standalone /app/api

RUN python -m venv /env && \
    . /env/bin/activate && \
    pip install --upgrade pip && \
    pip install wheel && \
    poetry install --with examples

EXPOSE 5000

# /bin/bash -c "/env/bin/uvicorn api:app --ws auto --host 0.0.0.0 --port 5000 --reload --log-config ./api/logging.json"
