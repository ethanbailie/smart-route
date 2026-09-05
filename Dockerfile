# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG GIT_SHA=unknown
ARG BUILT_AT=unknown
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/ethan/smart-route/internal/buildinfo.Version=$VERSION -X github.com/ethan/smart-route/internal/buildinfo.GitSHA=$GIT_SHA -X github.com/ethan/smart-route/internal/buildinfo.BuiltAt=$BUILT_AT" -o /smart-route ./cmd/smart-route

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && mkdir -p /var/lib/smart-route && chown 65532:65532 /var/lib/smart-route
COPY --from=build /smart-route /usr/local/bin/smart-route
USER 65532:65532
EXPOSE 8080
VOLUME ["/var/lib/smart-route"]
ENTRYPOINT ["smart-route"]
CMD ["serve"]
