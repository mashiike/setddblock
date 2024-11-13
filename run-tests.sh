#!/bin/bash

# Exit on error and unset variables
set -eu

# Set up environment variables
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export DYNAMODB_LOCAL_ENDPOINT=http://localhost:8000
export AWS_DEFAULT_REGION=ap-northeast-1

# Display usage information
usage() {
  echo "Usage: $0 [test_file]"
  echo "Run all tests if no test file is specified."
  echo "Options:"
  echo "  -h, --help     Display this help message"
}

# Check if help is requested
if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Invalid option: $1"
      usage
      exit 1
      ;;
  esac
fi

# Check for a test file argument
TEST_FILE="${1:-all}"

# Start DynamoDB Local using Docker Compose
echo "Starting DynamoDB Local..."
docker-compose up -d ddb-local

# Wait for DynamoDB Local to be ready
echo "Waiting for DynamoDB Local to be ready..."
until curl -s http://localhost:8000; do
  sleep 1
done
echo "DynamoDB Local is ready."

# Run tests and capture the exit code
echo "Running tests for the setddblock package..."
if [[ "$TEST_FILE" == "all" ]]; then
  go test -v -race -timeout 30s ./... || test_status=$?
else
  go test -v -race -timeout 30s "$TEST_FILE" || test_status=$?
fi

# Default exit status to 0 if tests passed
test_status="${test_status:-0}"

# Stop DynamoDB Local
echo "Stopping DynamoDB Local..."
docker-compose down

# Exit with the test command's status
exit "$test_status"
