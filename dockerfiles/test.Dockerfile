FROM python:3.9 AS base

RUN curl -L https://get.helm.sh/helm-v3.4.2-linux-amd64.tar.gz \
        | tar -xvz --strip-components 1 -C /usr/local/bin linux-amd64/helm &&\
    chmod +x /usr/local/bin/helm

ARG gitlab_user
ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1 \
    TZ="UTC" \
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_PREFER_BINARY=1 \
    POETRY_VERSION=1.4.1 \
    POETRY_HTTP_BASIC_GITLAB_USERNAME=$gitlab_user

COPY pyproject.toml poetry.lock ./

RUN pip install -q "poetry==$POETRY_VERSION"
RUN poetry self add -q keyrings.google-artifactregistry-auth@latest


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
    poetry install --no-root --only main; \
    python -m venv .venv \
    && poetry install --no-root \
    && poetry env info \
    && rm pyproject.toml poetry.lock

RUN apt-get -q update && apt-get -qy install netcat-openbsd
