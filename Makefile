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

all: push

#
# Docker tag with v prefix to differentiate the official release build, triggered by git tagging
#
TAG ?= v0.0.7
PREFIX ?= datastax/pulsar-heartbeat
BUILD_DIR ?= bin

build:
	go build -o $(BUILD_DIR)/pulsar-heartbeat src/main.go

test:
	go test ./...

container:
	docker build -t $(PREFIX):$(TAG) .
	docker tag $(PREFIX):$(TAG) ${PREFIX}:latest

push: container
	docker push $(PREFIX):$(TAG)
	docker push $(PREFIX):latest

clean:
	rm $(BUILD_DIR)/*
	docker rmi $(PREFIX):$(TAG)
