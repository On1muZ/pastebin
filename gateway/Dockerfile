FROM golang:1.27-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY gateway ./gateway
COPY gen ./gen
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/app ./gateway/cmd/gateway


FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/bin/app .
EXPOSE 8090

ENTRYPOINT ["./app"]

