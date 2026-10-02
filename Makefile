default: build
module=keda
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell curl -s https://go.dev/dl/?mode=json | jq -r '.[0].version' | sed 's/go//')
ALPINE_VERSION="3.24"
clean:
	rm ./${module}
	rm -rf vendor
	
update-packages:
	go mod edit -go=${GO_VERSION}
	go get -u ./...
	go mod tidy

fix:
	go fix ./...

lint:
	golangci-lint run 

lint-strict:
	gocritic check -enableAll ./...
	
run:
	go run .
	
test:
	go test ./...

test-clean:
	go clean -testcache
	go test ./...

test-cover:
	go test ./... -coverprofile cover.out
	go tool cover -html=cover.out

test-fuzz:
	# go test -fuzz=FuzzValidate -fuzztime 60s  -tags=fuzzing

benchmarks:
	go test -benchmem  -bench=. ./... -tags=benchmark

build:
	go build -o ${module}  -ldflags="-X main.Version=${GIT_COMMIT}"

race:
	go build -race -o ${module}

tidy:
	go mod tidy

docker:
	docker build -t ${module} .

pre-image:
	go mod vendor

post-image:
	rm -rf vendor

image: tidy
	$(MAKE) pre-image
	docker build -t ${module}:${GIT_COMMIT} --build-arg alpine_version=$(ALPINE_VERSION) --build-arg golang_version=${GO_VERSION} .
	$(MAKE) post-image	