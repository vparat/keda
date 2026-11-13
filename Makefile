.PHONY: default
default: build
module=keda
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell curl -s https://go.dev/dl/?mode=json | jq -r '.[0].version' | sed 's/go//')
ALPINE_VERSION="3.24"
.PHONY: clean
clean:
	@if [ -e "./$(module)" ]; then rm "./$(module)"; fi
	@if [ -e vendor ]; then rm -rf vendor; fi
	@if [ -e cover.out ]; then rm cover.out; fi

.PHONY: clean-images
clean-images:
	docker image ls $(module)
	@images=$$(docker image ls $(module) --format '{{.Repository}}:{{.Tag}}') || exit $$?; \
	if [ -n "$$images" ]; then \
		docker rmi $$images; \
	fi
	
.PHONY: update-packages
update-packages:
	go mod edit -go=${GO_VERSION}
	go get -u ./...
	go mod tidy

.PHONY: fix
fix:
	go fix ./...

.PHONY: lint
lint:
	golangci-lint run 

.PHONY: lint-strict
lint-strict:
	gocritic check -enableAll ./...
	
.PHONY: run
run:
	go run .
	
.PHONY: test
test:
	go test ./...

.PHONY: test-clean
test-clean:
	go clean -testcache
	go test ./...

.PHONY: test-cover
test-cover:
	go test ./... -coverprofile cover.out
	go tool cover -html=cover.out

.PHONY: test-fuzz
test-fuzz:
	# go test -fuzz=FuzzValidate -fuzztime 60s  -tags=fuzzing

.PHONY: benchmarks
benchmarks:
	go test -benchmem  -bench=. ./... -tags=benchmark

.PHONY: build
build:
	go build -o ${module}  -ldflags="-X main.Version=${GIT_COMMIT}"

.PHONY: race
race:
	go build -race -o ${module}

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: docker
docker:
	docker build -t ${module} .

.PHONY: pre-image
pre-image:
	go mod vendor

.PHONY: post-image
post-image:
	rm -rf vendor

.PHONY: image
image: tidy
	$(MAKE) pre-image
	docker build -t ${module}:${GIT_COMMIT} --build-arg alpine_version=$(ALPINE_VERSION) --build-arg golang_version=${GO_VERSION} .
	$(MAKE) post-image	

.PHONY: image-gcloud
image-gcloud: tidy
	$(MAKE) pre-image
	docker build -t ${module}:${GIT_COMMIT} --build-arg alpine_version=$(ALPINE_VERSION) --build-arg golang_version=${GO_VERSION} -f ./Dockerfile-gcloud .
	$(MAKE) post-image	

.PHONY: kind-load-image
kind-load-image:
	kind load docker-image ${module}:${GIT_COMMIT}
