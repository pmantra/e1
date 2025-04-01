# this is what's used in GCP cloud sql
FROM postgres:12.3

ENV POSTGRES_HOST_AUTH_METHOD trust
