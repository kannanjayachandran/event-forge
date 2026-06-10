FROM golang:1.22-alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /sim ./cmd/sim

FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /sim /usr/local/bin/sim
COPY configs/config.yaml /etc/sim/config.yaml
EXPOSE 8080
ENTRYPOINT ["sim"]
CMD ["--config", "/etc/sim/config.yaml"]
