FROM python:3.12.12-slim-bookworm
LABEL maintainer="zenpaiang <zenpai@2020220.xyz>"
LABEL org.opencontainers.image.source=https://github.com/whiteout-project/bot

COPY bootstrap.sh /bootstrap.sh

RUN set -x
RUN apt-get update && apt-get install --no-install-recommends -y curl ca-certificates
RUN pip install --upgrade pip
RUN mkdir -p /app
RUN curl -o /app/install.py https://raw.githubusercontent.com/whiteout-project/install/refs/heads/main/install.py
RUN cd /app; python install.py
RUN chmod +x /bootstrap.sh
RUN apt-get -y autoremove && apt-get -y autoclean && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app
ENTRYPOINT ["/bin/sh", "/bootstrap.sh"]