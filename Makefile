ifeq ($(OS),Windows_NT)
BINARY := websub-relayd.exe
else
BINARY := websub-relayd
endif

.PHONY: build test vet fmt interop clean

build:
	go build -o $(BINARY) ./cmd/websub-relayd

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f websub-relayd websub-relayd.exe

# Requires the canonical Python checkout (set RELAY_SRC) and uv.
RELAY_SRC ?= ../websub-relay/src
interop: build
	uv run --with aiohttp --with websockets \
		interop/interop.py --go-binary ./$(BINARY) --relay-src $(RELAY_SRC)
