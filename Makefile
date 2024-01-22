DOCKER_USER_NAME ?= ${DOCKERHUB_USERNAME}
APPLICATION_NAME ?= earthquake_data_layer

GIT_HASH ?= $(shell git log --format="%h" -n 1)
VERSION := $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)
_BUILD_ARGS_TAG ?= $(GIT_HASH)
_BUILD_ARGS_RELEASE_TAG ?= latest
_BUILD_ARGS_DOCKERFILE ?= Dockerfile

setup:
	apt install pipx -y
	pipx install poetry==1.5.1
	poetry install --without dev

_builder:
	docker build --tag ${APPLICATION_NAME}:${_BUILD_ARGS_TAG} -f ${_BUILD_ARGS_DOCKERFILE} .

_pusher:
	docker push ${DOCKER_USER_NAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG}

_releaser:
	docker pull ${DOCKER_USER_NAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG}
	docker tag  ${DOCKER_USER_NAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG} ${DOCKER_USER_NAME}/${APPLICATION_NAME}:latest
	docker tag  ${DOCKER_USER_NAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG} ${DOCKER_USER_NAME}/${APPLICATION_NAME}:${VERSION}
	docker push ${DOCKER_USER_NAME}/${APPLICATION_NAME} --all-tags

build:
	# poetry lock
	$(MAKE) _builder \
	-e _BUILD_ARGS_TAG="$*$(_BUILD_ARGS_TAG)" \
	-e _BUILD_ARGS_DOCKERFILE="Dockerfile"

integration_test: build
	LOCAL_IMAGE_NAME=${APPLICATION_NAME}:${_BUILD_ARGS_TAG} bash integration_tests/run.sh

push: integration_test build
	$(MAKE) _pusher \
	-e _BUILD_ARGS_TAG="$*${GIT_HASH}"

release: push integration_test build
	$(MAKE) _releaser \
	-e _BUILD_ARGS_TAG="$*${_BUILD_ARGS_TAG}" \
	-e _BUILD_ARGS_RELEASE_TAG="$*latest"
