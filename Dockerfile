# multi-stage build
FROM debian:11 AS proot
RUN apt-get update && apt-get install -q -y build-essential git libseccomp-dev libtalloc-dev \
 # deps for PERSISTENT_CHOWN extension
 libprotobuf-c-dev libattr1-dev
RUN git clone https://github.com/rootless-containers/PRoot.git \
  && cd PRoot \
  && git checkout 081bb63955eb4378e53cf4d0eb0ed0d3222bf66e \
  && cd src \
  && make && mv proot / && make clean

FROM golang:1.24-alpine AS runc
RUN apk add --no-cache git g++ linux-headers
RUN go install github.com/opencontainers/runc@latest

#
# build stage
#
FROM golang:1.24-alpine AS builder

# Add Maintainer Info
LABEL maintainer="ming"

RUN apk --no-cache add build-base git gcc

WORKDIR /root/

# Go debugging tools
RUN go install github.com/google/gops@latest

# pre-copy/cache go.mod for downloading dependencies and only re-download them in subsequent builds if dependencies change
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build make build

#
# Start a new stage from scratch
#
FROM alpine:3.21
RUN adduser -u 1000 -S user -G root
COPY --from=proot --chown=1000:0 /proot /home/user/.runrootless/runrootless-proot
COPY --from=runc --chown=1000:0 /go/bin/runc /home/user/bin/runc

RUN apk --no-cache add ca-certificates
COPY --from=builder --chown=1000:0 /root/bin/pulsar-heartbeat /home/user

# Copy debug tools
COPY --from=builder --chown=1000:0 /go/bin/gops /home/user/gops
RUN mkdir /home/user/run && chmod g=u /home/user/run
RUN chmod g=u /home/user
USER 1000:0
WORKDIR /home/user
ENV HOME=/home/user
ENV PATH=/home/user/bin:$PATH
ENV XDG_RUNTIME_DIR=/home/user/run
ENV PULSAR_OPS_MONITOR_CFG=/config/runtime.yml
ENTRYPOINT ./pulsar-heartbeat
