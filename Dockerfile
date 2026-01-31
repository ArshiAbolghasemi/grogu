FROM docker.mci.dev/golang:1.25-alpine AS builder

ENV GOPROXY="https://repo.mci.dev/artifactory/api/go/goproxy-virtual"
ENV GO111MODULE=on
WORKDIR /app

RUN --mount=type=bind,source=./go.mod,target=/app/go.mod \
    --mount=type=bind,source=./go.sum,target=/app/go.sum \
    go mod download

COPY . .

RUN mkdir -p bin && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -o bin \
    -ldflags="-s -w" \
    -trimpath \
    -tags "release" \
    -buildvcs=false \
    ./cmd/*


FROM docker.mci.dev/alpine:3

WORKDIR /app
RUN apk add --no-cache iputils curl mtr tzdata ffmpeg

COPY ./migrations ./migrations
COPY --from=builder /app/bin/ /app/
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENV GIN_MODE=release
EXPOSE 2112

ENTRYPOINT ["/app/entrypoint.sh"]
