ARG alpine_version=latest
ARG golang_version=1.27

FROM golang:${golang_version}-alpine${alpine_version} AS builder

WORKDIR $GOPATH/src/github.com/vparat/keda
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -o /home/keda

FROM alpine:${alpine_version}

RUN apk update && apk add --no-cache jq kubectl && apk upgrade --no-cache && rm -rf /var/cache/apk/*

COPY --from=builder /home/keda /keda

RUN adduser  -D -h /home/keda -u 1001 -s /sbin/nologin  keda
RUN mkdir -p /home/workdir/ && chown -R keda:keda /home/workdir
WORKDIR /home/workdir
USER keda

ENTRYPOINT ["/keda"]