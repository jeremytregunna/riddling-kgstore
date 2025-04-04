#!/bin/bash

echo "Running benchmarks..."
go test -bench=. -benchmem ./...

# If the --profile flag is provided, generate CPU and memory profiles
if [ "$1" == "--profile" ]; then
    echo -e "\nRunning benchmarks with CPU profiling..."
    go test -bench=. -benchmem -cpuprofile=cpu.prof ./...
    
    echo -e "\nRunning benchmarks with memory profiling..."
    go test -bench=. -benchmem -memprofile=mem.prof ./...
    
    echo -e "\nProfiles generated: cpu.prof, mem.prof"
    echo "View profiles with: go tool pprof [cpu.prof|mem.prof]"
fi