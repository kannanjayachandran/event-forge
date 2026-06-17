FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /sim ./cmd/sim

FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /sim /usr/local/bin/sim

COPY configs/config.yaml /etc/sim/config.yaml

EXPOSE 8080

ENTRYPOINT ["sim"]
CMD ["--config", "/etc/sim/config.yaml"]
