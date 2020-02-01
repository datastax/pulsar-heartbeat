# multi-stage build

#
# build stage
#a
FROM golang:alpine AS builder

# Add Maintainer Info
LABEL maintainer="ming"

RUN apk --no-cache add build-base git bzr mercurial gcc
WORKDIR /root/
ADD . /root
RUN cd /root/src && go build -o pulsar-monitor

#
# Start a new stage from scratch
#
FROM alpine
WORKDIR /app
COPY --from=builder /root/src/pulsar-monitor /app/
ENTRYPOINT ./pulsar-monitor ./runtime.json