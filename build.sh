#!/bin/bash

# SiteMerge Build Script
# This script builds the SiteMerge CLI tool for multiple platforms

set -e

echo "🚀 Building SiteMerge CLI Tool..."

# Get version from git or use default
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build flags
LDFLAGS="-s -w -X main.Version=$VERSION -X main.BuildTime=$BUILD_TIME -X main.GitCommit=$GIT_COMMIT"

# Create build directory
mkdir -p bin

# Build for current platform
echo "📦 Building for current platform..."
go build -ldflags "$LDFLAGS" -o bin/sitemerge .

# Build for multiple platforms if requested
if [ "$1" = "all" ]; then
    echo "📦 Building for multiple platforms..."
    
    # Linux AMD64
    echo "  Building for Linux AMD64..."
    GOOS=linux GOARCH=amd64 go build -ldflags "$LDFLAGS" -o bin/sitemerge-linux-amd64 .
    
    # Linux ARM64
    echo "  Building for Linux ARM64..."
    GOOS=linux GOARCH=arm64 go build -ldflags "$LDFLAGS" -o bin/sitemerge-linux-arm64 .
    
    # macOS AMD64
    echo "  Building for macOS AMD64..."
    GOOS=darwin GOARCH=amd64 go build -ldflags "$LDFLAGS" -o bin/sitemerge-darwin-amd64 .
    
    # macOS ARM64 (Apple Silicon)
    echo "  Building for macOS ARM64..."
    GOOS=darwin GOARCH=arm64 go build -ldflags "$LDFLAGS" -o bin/sitemerge-darwin-arm64 .
    
    # Windows AMD64
    echo "  Building for Windows AMD64..."
    GOOS=windows GOARCH=amd64 go build -ldflags "$LDFLAGS" -o bin/sitemerge-windows-amd64.exe .
fi

echo "✅ Build completed successfully!"
echo ""
echo "📁 Build artifacts:"
ls -la bin/

echo ""
echo "🎯 Usage:"
echo "  ./bin/sitemerge --help"
echo ""
echo "Example commands:"
echo "  ./bin/sitemerge table-index --host 127.0.0.1 --port 4000 --user root --password pass --database mydb"
echo "  ./bin/sitemerge page-info --host 127.0.0.1 --port 4000 --user root --password pass --database mydb --page-size 1000" 
echo "  ./bin/sitemerge export --host 127.0.0.1 --port 4000 --user root --password pass --database mydb --table mytable"
echo "  ./bin/sitemerge import --host 127.0.0.1 --port 4000 --user root --password pass --database mydb --table mytable --threads 10"