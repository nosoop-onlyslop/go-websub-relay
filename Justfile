# Justfile for go-websub-relay.
#
# Run `just` to list recipes, or `just build`, `just test`, `just cross`, etc.

set shell := ["bash", "-uc"]
set windows-shell := ["bash", "-uc"]

binary := if os_family() == "windows" { "websub-relayd.exe" } else { "websub-relayd" }
relay_src := env_var_or_default("RELAY_SRC", "../websub-relay/src")

# Build the daemon for the host platform.
default: build

# Build the daemon for the host platform.
build:
    go build -o {{binary}} ./cmd/websub-relayd

# Run all tests.
test:
    go test ./...

# Run go vet.
vet:
    go vet ./...

# Format all Go sources.
fmt:
    gofmt -w .

# Remove build artefacts.
clean:
    rm -rf dist {{binary}}

# Cross-compile static Linux binaries for amd64 and arm64 into dist/.
cross: (cross-linux "amd64") (cross-linux "arm64")

# Cross-compile a static Linux binary for one architecture: just cross-linux arm64
cross-linux arch:
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH={{arch}} go build -trimpath \
        -o dist/websub-relayd-linux-{{arch}} ./cmd/websub-relayd

# Cross-language interop test; requires the canonical Python checkout and uv.
interop: build
    uv run --with aiohttp --with websockets --with msgspec \
        interop/interop.py --go-binary ./{{binary}} --relay-src {{relay_src}}
