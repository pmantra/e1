SHELL := bash

.PHONY=python
python:
	python -m grpc_tools.protoc -I. \
		--python_out=generated/python \
		--grpclib_python_out=generated/python \
		--grpc_python_out=generated/python \
		maven-schemas/*.proto

all: python