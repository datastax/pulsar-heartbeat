# multi-stage build
FROM debian:9 AS proot
RUN apt-get update && apt-get install -q -y build-essential git libseccomp-dev libtalloc-dev \
 # deps for PERSISTENT_CHOWN extension
 libprotobuf-c-dev libattr1-dev
RUN git clone https://github.com/rootless-containers/PRoot.git \
  && cd PRoot \
  && git checkout 081bb63955eb4378e53cf4d0eb0ed0d3222bf66e \
  && cd src \
  && make && mv proot / && make clean

FROM golang:1.9-alpine AS runc
RUN apk add --no-cache git g++ linux-headers
RUN git clone https://github.com/opencontainers/runc.git /go/src/github.com/opencontainers/runc \
  && cd /go/src/github.com/opencontainers/runc \
  && git checkout -q e6516b3d5dc780cb57a976013c242a9a93052543 \
  && go build -o /runc .

#
# build stage
#
FROM golang:alpine AS builder

# Add Maintainer Info
LABEL maintainer="ming"

RUN apk --no-cache add build-base git gcc
WORKDIR /root/
ADD . /root
RUN cd /root/src && go build -o pulsar-heartbeat

# Add debug tool
RUN go get github.com/google/gops

#
# Start a new stage from scratch
#
FROM alpine:3.7
RUN adduser -u 1000 -D user
COPY --from=proot /proot /home/user/.runrootless/runrootless-proot
COPY --from=runc /runc /home/user/bin/runc

RUN apk --no-cache add ca-certificates
COPY --from=builder /root/src/pulsar-heartbeat /home/user

# a kesque cert but it can overwritten by mounting the same path ca-bundle.crt
COPY --from=builder /root/config/kesque-pulsar.cert /etc/ssl/certs/ca-bundle.crt

# Copy debug tools
COPY --from=builder /go/bin/gops /home/user/gops
RUN mkdir /home/user/run
RUN chown -R user:0 /home/user && chmod g=u /home/user
USER user
WORKDIR /home/user
ENV HOME=/home/user
ENV PATH=/home/user/bin:$PATH
ENV XDG_RUNTIME_DIR=/home/user/run
ENV PULSAR_OPS_MONITOR_CFG=/config/runtime.yml
ENTRYPOINT ./pulsar-heartbeat
