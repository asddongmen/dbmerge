# SiteMerge Makefile
# This Makefile provides convenient targets for building the SiteMerge CLI tool

.PHONY: build build-all clean help

# Default target
build:
	@bash build.sh

# Build for all platforms
build-all:
	@bash build.sh all

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf bin/
	@echo "✅ Clean completed!"

# Show help
help:
	@echo "SiteMerge Build Targets:"
	@echo "  build      - Build for current platform (default)"
	@echo "  build-all  - Build for all supported platforms"
	@echo "  clean      - Remove build artifacts"
	@echo "  help       - Show this help message"

# Make 'build' the default target
.DEFAULT_GOAL := build 