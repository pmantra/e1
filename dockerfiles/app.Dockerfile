FROM python:3.9 AS base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1 \
    TZ="UTC"

FROM base as builder_base

ARG gitlab_token
ARG gitlab_user
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_PREFER_BINARY=1 \
    POETRY_VERSION=1.4.1 \
    POETRY_HTTP_BASIC_GITLAB_USERNAME=$gitlab_user

RUN pip install "poetry==$POETRY_VERSION"
RUN poetry self add -q keyrings.google-artifactregistry-auth@latest
RUN python -m venv .venv

FROM builder_base as builder

COPY pyproject.toml poetry.lock ./


RUN --mount=type=secret,id=gitlab_token \
    --mount=type=secret,id=gcloud_adc \
    --mount=type=secret,id=gcp_access_token \
    export POETRY_HTTP_BASIC_GITLAB_PASSWORD="$(cat /run/secrets/gitlab_token)"; \
    if [ -s /run/secrets/gcp_access_token ]; then \
        export POETRY_HTTP_BASIC_PYPI_MVN_USERNAME=oauth2accesstoken; \
        export POETRY_HTTP_BASIC_PYPI_MVN_PASSWORD="$(cat /run/secrets/gcp_access_token)";  \
    fi; \
    \
    if [ -s /run/secrets/gcloud_adc ]; then \
      export GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/gcloud_adc; \
    fi; \
    poetry install --no-root --only main
    
FROM base as runnable

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        telnet \
    && rm -rf /var/lib/apt/lists/*

RUN GRPC_HEALTH_PROBE_VERSION=v0.3.3 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

FROM runnable as final

COPY --from=builder .venv /.venv

RUN useradd -ms /bin/bash maven
USER maven
RUN mkdir /home/maven/app
WORKDIR /home/maven/app

COPY bin bin
# do not copy the whole config directory, only copy the specific
# python files. read the large comment in config/settings.py to see why
COPY config/settings.py config/__init__.py config/
COPY db db
COPY constants.py .
COPY main.py .
COPY api api
COPY app app
COPY utils utils
COPY ingestion ingestion
COPY split split
COPY verification verification
COPY tests tests
COPY http_api http_api

# GRPC
EXPOSE 50051/tcp
# ADMIN
EXPOSE 5000/tcp

ENV DD_TRACE_LOGGING_ENABLED false
ENV DD_LOGS_INJECTION false

ENTRYPOINT ["/.venv/bin/ddtrace-run", "/.venv/bin/python", "main.py"]
