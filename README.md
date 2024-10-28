# Verve App

## Overview
This is a high-throughput application designed to handle 10,000+ requests per second.

## Running the Application

### Prerequisites
- Docker
- Go 1.18

To build and run the application:

```bash
docker build -t verve-app .
docker run -p 8080:8080 verve-app
