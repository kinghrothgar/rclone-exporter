# Use Golang base image for building the binary
FROM golang:1.24-alpine AS builder
WORKDIR /app
# Install git for module fetching
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o rclone-exporter .

# Create a minimal final image
FROM alpine:3.21
COPY --from=builder /app/rclone-exporter /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rclone-exporter"]
