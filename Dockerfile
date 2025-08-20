FROM golang:1.25 as builder

WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/wb-service

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/wb-service .
COPY --from=builder /app/.env .

EXPOSE 8080
CMD ["./wb-service"]