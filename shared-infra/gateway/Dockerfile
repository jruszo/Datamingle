FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /gateway ./main.go

FROM alpine:3.21

RUN apk --no-cache add ca-certificates

COPY --from=builder /gateway /gateway

EXPOSE 4430

ENTRYPOINT ["/gateway"]
