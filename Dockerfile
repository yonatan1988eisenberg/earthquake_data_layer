# version 0.1

FROM python:3.10-slim

# WORKDIR /app

COPY collect.py pyproject.toml poetry.lock Makefile README.md ./
COPY earthquake_data_layer ./earthquake_data_layer/

ENV PATH="/root/.local/bin:$PATH"

RUN apt-get update && \
    apt-get install make -y && \
    make setup && \
    rm -rf /var/lib/apt/lists/*

CMD ["poetry", "run", "python3", "-m", "earthquake_data_layer.entrypoint"]

EXPOSE 1317
