# multi-stage build

#
# build stage
#a
FROM golang:alpine AS builder

# Add Maintainer Info
LABEL maintainer="ming"

RUN apk --no-cache add build-base git gcc
WORKDIR /root/
ADD . /root
RUN cd /root/src && go build -o pulsar-monitor

#
# Start a new stage from scratch
#
FROM alpine
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /root/src/pulsar-monitor /app/
COPY --from=builder /root/config/kesque-pulsar.cert /etc/ssl/certs/ca-bundle.crt
ENTRYPOINT ./pulsar-monitor ./runtime.yml