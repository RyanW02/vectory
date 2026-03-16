GO ?= GOEXPERIMENT=simd go

BENCH_FLAGS := -bench=.
TEST_FLAGS := -v

.DEFAULT_GOAL := help

help: ## Show available tasks
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS=":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

test: ## Run tests
	@echo "Running tests..."
	$(GO) test $(TEST_FLAGS) ./...

test-cover: TEST_FLAGS += -coverpkg=./... -coverprofile=coverage.out
test-cover: test ## Run tests with coverage report
	@echo "Generating coverage report..."
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench: ## Run benchmarks
	@echo "Running benchmarks..."
	$(GO) test $(BENCH_FLAGS) ./...