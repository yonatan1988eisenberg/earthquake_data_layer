DOCKER_USERNAME ?= earthquakepredictor
APPLICATION_NAME ?= earthquake_data_layer

GIT_HASH ?= $(shell git log --format="%h" -n 1)
DATE = ${shell date +"%Y_%m_%d_%H_%M_%S"}
_BUILD_ARGS_TAG ?= $(GIT_HASH)_$(DATE)
_BUILD_ARGS_RELEASE_TAG ?= latest
_BUILD_ARGS_DOCKERFILE ?= Dockerfile

setup:
	apt install pipx -y
	pipx install poetry==1.5.1
	poetry install --without dev

_builder:
	docker build --tag ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG} -f ${_BUILD_ARGS_DOCKERFILE} .

_pusher:
	docker push ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG}

_releaser:
	docker pull ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG}
	docker tag  ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG} ${DOCKER_USERNAME}/${APPLICATION_NAME}:latest
	docker push ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_RELEASE_TAG}

build:
	# poetry lock
	$(MAKE) _builder \
	-e _BUILD_ARGS_TAG="$*$(_BUILD_ARGS_TAG)" \
	-e _BUILD_ARGS_DOCKERFILE="Dockerfile"

integration_test: build
	LOCAL_IMAGE_NAME=${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_TAG} bash integration_tests/run.sh

push_%:
	$(MAKE) _pusher \
	-e _BUILD_ARGS_TAG="$*${GIT_HASH}"

release: build integration_test
	$(MAKE) _releaser \
	-e _BUILD_ARGS_TAG="$*${_BUILD_ARGS_TAG}" \
	-e _BUILD_ARGS_RELEASE_TAG="$*latest"
