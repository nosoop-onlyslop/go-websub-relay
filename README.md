# go-websub-relay

A Go port of the canonical Python `websub-relay` project.  It covers both the
relay daemon and the shared client-facing protocol and topic helpers.  The
daemon is a drop-in replacement for the Python relay process: existing Python
clients and tenants can connect unchanged, and it reads the same `relay.toml`
configuration and SQLite database.

The canonical implementation (and the canonical wire protocol) remains in the
Python repository.  This project tracks it; when the protocol changes,
`protocol.ProtocolVersion` in both projects must be bumped together.

## Compatibility

- **TOML configuration** — the same file works with either daemon, including
  ISO 8601 durations (`renew_margin`, `retention`, `manager_interval`) and
  strict rejection of unknown fields.
- **Control protocol** — the JSON tagged-union messages (`hello`, `watch.sync`,
  `resume`, `ack`, `ping` / `welcome`, `watch.state`, `notification`, `error`,
  `pong`) are encoded exactly as `msgspec` does, so a Python client built
  against `websub_relay.protocol` interoperates as-is.
- **Go client support** — the `protocol` and `topics` packages are reusable
  outside the daemon.  A Go client can encode client messages, decode server
  messages, and verify delivery signatures with the same code the relay uses.
- **SQLite schema** — an existing `relay.db3` written by the Python daemon can
  be opened directly (including Python's `json.dumps`-style `routing` values and
  `+00:00` microsecond timestamps).

## Using the client-facing packages

The port includes the Go equivalents of `websub_relay.protocol` and
`websub_relay.topics`, so a client application can import them instead of
vendoring the Python modules.  They provide everything needed to speak the
control protocol, but not a turnkey client runtime.

A client is expected to:

1. dial the relay's `/v1/control` WebSocket endpoint;
2. send `protocol.Hello` and validate the returned `protocol.Welcome`
   (`ProtocolVersion`, `ResumeOK`, `OldestAvailableID`);
3. send `protocol.Resume` and then a full `protocol.WatchSync`;
4. handle `protocol.WatchStateUpdate`, `protocol.Notification`,
   `protocol.ErrorMessage` and `protocol.Pong`;
5. verify `Notification.Signature` with `protocol.VerifyDeliverySignature`;
6. persist/process each notification, then acknowledge it with `protocol.Ack`.

The relay delivers at least once and waits for an ack before sending the next
notification for that tenant.  Ack only after durable handling and deduplicate
by `Notification.ID`.  If `Welcome.ResumeOK` is false, the requested cursor is
older than the retained deliveries, so refetch state instead of relying on
replay.  Send feed URLs as-is in `WatchTarget.Topic`; topic translation and
normalization are the relay's responsibility.

Client transport concerns — reconnecting, cursor persistence, deduplication,
ping scheduling, and feed parsing — are intentionally left to the application.

## Running the daemon

```sh
go build -o websub-relayd ./cmd/websub-relayd
cp relay.example.toml relay.toml
# edit relay.toml
./websub-relayd --config relay.toml
```

The relay only needs to be reachable at `public_url` by the hub.  Keep
`listen` on loopback and terminate TLS with a reverse proxy.

`listen` accepts either a TCP address (`host:port`) or a Unix domain socket
(`unix:/path/to.sock`).  With a Unix socket, point your reverse proxy at the
socket file and make sure the proxy user has write access to it.  A stale
socket is removed on startup.

## Endpoints

- `GET /websub/cb/{token}` — hub subscription verification.
- `POST /websub/cb/{token}` — hub content delivery (signature verified).
- `GET /v1/control` — client WebSocket control channel.
- `GET /healthz` — health/status.

## Layout

| Path | Purpose |
| --- | --- |
| `cmd/websub-relayd` | command-line entry point |
| `config` | TOML parsing and ISO 8601 durations |
| `protocol` | control-protocol messages, encoders/decoders, and HMAC helpers (client-usable) |
| `topics` | publisher-specific topic helpers (client-usable) |
| `hub` | WebSub hub discovery / subscribe / unsubscribe |
| `store` | SQLite persistence |
| `relay` | subscription manager, callbacks, per-tenant delivery |
| `server` | HTTP and WebSocket endpoints |

## Tests

```sh
go test ./...
```

The protocol tests pin the JSON encoding to fixtures captured from the Python
implementation, and the server test exercises the full
subscribe/verify/push/notify/ack flow over a real WebSocket.

### Cross-language interop

`interop/interop.py` starts this daemon and drives it with a real WebSocket
client written against the **canonical Python protocol**, proving that Python
clients interoperate with the Go daemon.  It requires the canonical
`websub-relay` source tree (for `websub_relay.protocol`):

```sh
go build -o /tmp/websub-relayd ./cmd/websub-relayd
uv run --with aiohttp --with websockets \
    interop/interop.py \
    --go-binary /tmp/websub-relayd \
    --relay-src /path/to/websub-relay/src
```

### Reading a Python-produced database or config

The store and config packages can be pointed at files produced by the canonical
Python daemon:

```sh
RELAY_PYTHON_DB=/path/to/relay.db3 go test ./store/ -run TestOpenPythonDatabase -v
RELAY_PYTHON_CONFIG=/path/to/relay.toml go test ./config/ -run TestLoadPythonConfig -v
```
