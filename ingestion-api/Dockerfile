# Build Stage
FROM golang:1.25-alpine AS builder
WORKDIR /app

# Install git for private modules if needed
RUN apk add --no-cache git

# Copy go mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code from ingestion-api
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o ingestion-api-bin main.go

# Final Runtime Stage
FROM alpine:latest
WORKDIR /root/
RUN apk add --no-cache ca-certificates

# Copy the binary from the builder
COPY --from=builder /app/ingestion-api-bin .

# Expose Go API port
EXPOSE 8080

CMD ["./ingestion-api-bin"]