#!/usr/bin/env bash
# The script does automatic checking on a Go package and its sub-packages.
set -ex

env GORACE="halt_on_error=1" go test -race ./...

# Automatic checks
# Linters (staticcheck includes gosimple checks)
golangci-lint run --enable-only=staticcheck,unconvert,ineffassign,govet

# Formatter check
golangci-lint fmt --diff -E gofmt
