SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:

MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules


# compiles protobufs
protobufs:
	$(MAKE) -C api/protobufs/
.PHONY: protobufs

init-local:
	poetry install
.PHONY: init-local

forward-redisinsight:
	$(info Click to connect -> http://localhost:7000)
	kubectl port-forward service/eligibility-redisinsight-service 7000:8001
.PHONY: forward-redisinsight

forward-redis:
	$(info Connect with -> redis-cli -h localhost -p 7001 -a <password>)
	kubectl port-forward service/eligibility-redis-master 7001:6379
.PHONY: forward-redis

forward-db:
	$(info Connect with -> psql -d api -h localhost -p 7002 -U postgres -W <password>)
	cloud_sql_proxy -instances=maven-clinic-qa:us-central1:eligibility-0dba7367=tcp:7002
.PHONY: forward-db

# Standup the parts of e9y required for
mono-docker-up:
	docker-compose --profile=mono up --build -d

e9y-docker-up:
	docker-compose --profile=eligibility up --build -d

create-network:
	docker network create maven_default

docker-down:
	docker-compose --profile=all down

