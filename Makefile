# Copyright 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Try to use podman if docker isn't installed
CONTAINER_CMD := $(shell if command -v docker > /dev/null 2>&1; then \
                      echo docker; \
                   elif command -v podman > /dev/null 2>&1; then \
                      echo podman; \
                   else \
                      echo ""; \
                   fi)
ifeq ($(CONTAINER_CMD),)
$(warning "Neither docker nor podman is installed.")
endif

all: push

#
# Docker tag with v prefix to differentiate the official release build, triggered by git tagging
#
TAG ?= latest
PREFIX ?= datastax/pulsar-heartbeat
BUILD_DIR ?= bin
OUTPUT_FILE ?= pulsar-heartbeat

build:
	go build -o $(BUILD_DIR)/$(OUTPUT_FILE) src/main.go

test:
	go test ./...

container:
	$(CONTAINER_CMD) build -t $(PREFIX):$(TAG) .
	$(CONTAINER_CMD) tag $(PREFIX):$(TAG) ${PREFIX}:latest

push:
	$(CONTAINER_CMD) push $(PREFIX):$(TAG)
	$(CONTAINER_CMD) push $(PREFIX):latest

clean:
	rm $(BUILD_DIR)/*
	docker rmi $(PREFIX):$(TAG)

lint:
	golangci-lint run
