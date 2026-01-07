VERSION ?= $(shell cat _VERSION.txt)

MES_PKI = mes-int_1

GO_VERSION_FULL  = $(shell grep '^go ' go.mod | awk '{print $$2}')
GO_VERSION_SHORT = $(shell grep '^go ' go.mod | sed -n 's/^go \([^ ]*\)\.[^ ]*/\1/p')

MODULE_NAME     := $(shell cat go.mod | grep '^module' | cut -d ' ' -f 2)
COMMIT_HASH     := $(shell git rev-parse --short HEAD)
BUILD_TIMESTAMP := $(shell date -Iseconds)
LDFLAGS         =-s -w


.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Environment configuration

.PHONY: env-check
env-check: temp/env-check.ok ## Validate development environment

temp/env-check.ok:
	@if ! go version | grep $(GO_VERSION_SHORT) > /dev/null ; then echo "Go version $(GO_VERSION_FULL) required. See https://confluence.eis.n.mes.corp.hmrc.gov.uk/mdg/confluence/display/CDSScrum/How-To%3A+Go+language+stuff#How-To:Golanguagestuff-SwitchingVersions"; exit 1 ; fi
	@if ! command -v golangci-lint > /dev/null ; then echo "Cannot find golangci-lint. See https://golangci-lint.run/welcome/install/#local-installation" ; exit 1 ; fi
	@if ! command -v gomarkdoc > /dev/null ; then echo "Cannot find gomarkdoc. See https://github.com/princjef/gomarkdoc?tab=readme-ov-file#command-line-usage" ; exit 1 ; fi
	@mkdir -p temp
	@touch temp/env-check.ok

##@ Documentation

docs: lint ## Build API documetation
	gomarkdoc . > README.md

##@ Compilation/Code Generation

GOFILES := $(shell find . -name "*.go" -print)

temp/golangci-lint.ok: .golangci.yml $(GOFILES)
	@echo "Linting..."
	@golangci-lint run --timeout 2m30s ./...
	@mkdir -p temp
	@touch temp/golangci-lint.ok

.PHONY: lint
lint: temp/golangci-lint.ok	## Run linter over source code

##@ Testing

.PHONY: test
test: ## Run the unit tests
	@go test -v ./...
