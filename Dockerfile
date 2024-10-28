FROM golang:1.18

WORKDIR /app

# Copy the Go modules files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code into the container
COPY . .

RUN go build -o verve-app main.go

# Expose the port the app runs on
EXPOSE 8080

# Run the binary program created
CMD ["./verve-app"]
