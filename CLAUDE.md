# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Lint/Test Commands
- Build: Not yet established (Go implementation planned)
- Lint: `go fmt ./...` and `go lint ./...`
- Test: `go test ./...`
- Single test: `go test -v ./path/to/package -run TestName`
- Benchmark: `go test -bench=. ./...`

## Code Style Guidelines
- **Language**: Go
- **Formatting**: Use `go fmt` for automatic code formatting
- **Imports**: Group standard library imports first, then third-party packages, then local packages
- **Types**: Prefer strong typing; define structs for core data structures (Node, Edge, Property, Page)
- **Naming**:
  - Use CamelCase for exported names, camelCase for non-exported names
  - Use descriptive variable/function names
- **Error Handling**: Return errors explicitly, never use panic in production code
- **Logging**: Implement structured logging for operations
- **Comments**: Document all public functions and complex logic
- **Testing**: Maintain 100% test coverage for core components

## Project Structure
Single-file database format for knowledge graphs, full details in the ARCHITECTURE.md file.

## Module name
As you build more and more of the project, you must be aware when importing other pieces of code that the module name is correct, so read go.mod to figure that out.

## Implementing
As you implement the project, you will use and update the TODO.md file checking items off as you complete them. You will start in Phase 1, and for every item, iterate them one by one until completion. Once one item is complete, you must check it off in the TODO.md before progressing.
