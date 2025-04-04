#!/bin/bash

# Run tests with coverage
echo "Running tests with coverage..."
go test -coverprofile=coverage.out ./...

# Display coverage report
echo -e "\nCoverage report:"
go tool cover -func=coverage.out

# Generate HTML coverage report if flag is provided
if [ "$1" == "--html" ]; then
    echo -e "\nGenerating HTML coverage report..."
    go tool cover -html=coverage.out -o coverage.html
    echo "HTML coverage report generated: coverage.html"
fi

# Clean up if flag is provided
if [ "$1" == "--clean" ]; then
    echo -e "\nCleaning up coverage files..."
    rm -f coverage.out coverage.html
fi