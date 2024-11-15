#!/bin/bash

# Exit on error and unset variables
set -eu

# Initialize status variables
build_status=0
lint_status=0
test_status=0
cli_test_status=0
timeout_test_status=0
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
until curl -s $DYNAMODB_LOCAL_ENDPOINT; do
  sleep 1
done
echo "DynamoDB Local is ready."
echo

# Run tests and capture the exit code
echo
echo "Running tests for the setddblock package..."
if [[ "$TEST_FILE" == "all" ]]; then
  go test -v -race -timeout 30s ./... || test_status=$?
else
  go test -v -race -timeout 30s "$TEST_FILE" || test_status=$?
fi
echo

# Default exit status to 0 if tests passed
test_status="${test_status:-0}"

echo
echo "Running golangci-lint..."
golangci-lint run --out-format line-number --config=.golangci.yaml || lint_status=$?
echo
# Fail if the build status is non-zero
if [ "${lint_status:-0}" -ne 0 ]; then
    echo "ERROR: lint failed with status $lint_status"
fi


## simplified cli based test set so we can see and validate the cli log output changes
# Build the setddblock CLI tool
echo
echo "Building setddblock CLI tool..."
go build -o setddblock ./cmd/setddblock || build_status=$?
echo
# Fail if the build status is non-zero
if [ "${build_status:-0}" -ne 0 ]; then
    echo "ERROR: Build failed with status $build_status"
    exit $build_status
fi

echo
echo "Acquiring lock and running sleep command..."
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy ./setddblock -nX --endpoint $DYNAMODB_LOCAL_ENDPOINT ddb://test-table/test-item /bin/sh -c 'echo "Lock acquired! Sleeping..."; sleep 2' &
sleep .2
echo

# Attempt to acquire the lock again, which should fail
echo "Attempting to acquire lock again, which should fail..."
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy ./setddblock -nX --endpoint $DYNAMODB_LOCAL_ENDPOINT ddb://test-table/test-item /bin/sh -c 'echo "This should not print, lock should not be acquired"; exit 0' || cli_test_status=$?
echo
if [ "${cli_test_status:-0}" == 3 ]; then
    echo "Lock acquisition failed as expected: $cli_test_status"
    cli_test_status=0
else
    echo "Unexpected CLI test status: $cli_test_status"
fi
echo
# Wait for the background process to finish
echo "Waiting for original lock to release"
wait


echo "Testing --timeout"
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy ./setddblock -nX --timeout 1s --endpoint $DYNAMODB_LOCAL_ENDPOINT ddb://test-table/test-item /bin/sh -c 'echo "Lock acquired! Sleeping..."; sleep 10' || timeout_test_status=$?

if [ "${timeout_test_status:-0}" -eq 5 ]; then
    echo "Timeout test failed as expected with status $timeout_test_status"
    timeout_test_status=0
else
    echo "ERROR: Timeout test did not fail as expected, exit code $timeout_test_status"
fi


# Stop DynamoDB Local
echo "Stopping DynamoDB Local..."
docker-compose down

# Log the status of each step
if [ "$build_status" -ne 0 ]; then
  echo "Build failed with status $build_status"
  exit $build_status
fi

if [ "$lint_status" -ne 0 ]; then
  echo "ERROR: Linting failed with status $lint_status"
fi

if [ "$test_status" -ne 0 ]; then
  echo "ERROR: Tests failed with status $test_status"
fi

if [ "$cli_test_status" -ne 0 ]; then
  echo "ERROR: CLI test failed with status $cli_test_status"
fi

if [ "$timeout_test_status" -ne 0 ]; then
  echo "ERROR: Timeout test failed with status $timeout_test_status"
fi
exit=$((lint_status + test_status + cli_test_status + timeout_test_status))
if [[ $exit -gt 0 ]]
 then
  echo "ERROR: something failed"
else
  echo "Success: everything passed"
fi
# Exit with the combined status of lint, test, CLI test, and timeout test
exit $exit
