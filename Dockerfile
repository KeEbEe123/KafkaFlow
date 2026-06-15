FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/broker ./cmd/broker && go build -o /app/cli ./cmd/cli

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/broker /app/broker
COPY --from=builder /app/cli /app/cli
ENTRYPOINT ["/app/broker"]
