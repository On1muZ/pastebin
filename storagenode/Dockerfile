FROM golang:1.27-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY storagenode ./storagenode
COPY gen ./gen
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/app ./storagenode/cmd/storagenode/

FROM alpine:3.20
WORKDIR /app
RUN mkdir -p /data/
COPY --from=builder /app/bin/app .
EXPOSE 8050
ENTRYPOINT ["./app"]
CMD ["-addr", ":8050", "-data", "/data"]
