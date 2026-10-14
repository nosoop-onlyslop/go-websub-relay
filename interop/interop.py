#!/usr/bin/env python3
"""
Cross-language interop smoke test for go-websub-relay.

It starts the Go relay daemon, a fake WebSub hub, and a real WebSocket client
written against the *canonical* Python protocol (``websub_relay.protocol``).
It then drives the full subscribe / verify / push / notify / ack flow and
asserts that the Go daemon speaks the same wire protocol and reads the same
TOML configuration format as the Python implementation.

Run it with the Go daemon binary built, for example:

    go build -o /tmp/websub-relayd ./cmd/websub-relayd
    uv run --with aiohttp --with websockets \
        interop/interop.py --go-binary /tmp/websub-relayd \
        --relay-src ../websub-relay/src

``--relay-src`` points at the ``src`` directory of the canonical Python
``websub-relay`` checkout; it is where ``websub_relay.protocol`` is imported
from.  It may also be supplied through the ``WEBSUB_RELAY_SRC`` environment
variable.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

import aiohttp
from aiohttp import web
from websockets.asyncio.client import connect as ws_connect


def add_relay_src(path: str | None) -> None:
    candidates = []
    if path:
        candidates.append(pathlib.Path(path))
    env = os.environ.get("WEBSUB_RELAY_SRC")
    if env:
        candidates.append(pathlib.Path(env))
    candidates.append(pathlib.Path(__file__).resolve().parents[2] / "websub-relay" / "src")
    for candidate in candidates:
        if (candidate / "websub_relay" / "protocol.py").is_file():
            sys.path.insert(0, str(candidate))
            return
    raise SystemExit(
        "could not find websub_relay.protocol; pass --relay-src PATH or set WEBSUB_RELAY_SRC"
    )


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def start_fake_hub(state: dict[str, str]) -> tuple[web.AppRunner, int]:
    async def handle_subscribe(request: web.Request) -> web.Response:
        form = await request.post()
        callback = form["hub.callback"]
        state["callback"] = callback
        state["secret"] = form.get("hub.secret", "")
        params = {
            "hub.mode": form["hub.mode"],
            "hub.topic": form["hub.topic"],
            "hub.challenge": "test-challenge",
            "hub.lease_seconds": form.get("hub.lease_seconds", "3600"),
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(callback, params=params) as response:
                assert response.status == 200, response.status
                assert await response.text() == "test-challenge"
        return web.Response(status=202)

    app = web.Application()
    app.router.add_post("/", handle_subscribe)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    return runner, port


async def wait_for_health(url: str, proc: subprocess.Popen[bytes], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise SystemExit(f"relay exited early with code {proc.returncode}")
            try:
                async with session.get(f"{url}/healthz") as response:
                    if response.status == 200:
                        return
            except aiohttp.ClientError:
                pass
            await asyncio.sleep(0.1)
    raise SystemExit("relay did not become healthy in time")


async def run(args: argparse.Namespace) -> None:
    from websub_relay import protocol as proto
    from websub_relay import topics

    hub_state: dict[str, str] = {}
    hub_runner, hub_port = await start_fake_hub(hub_state)
    workdir = tempfile.mkdtemp(prefix="websub-relayd-interop-")
    relay_port = free_port()
    relay_url = f"http://127.0.0.1:{relay_port}"

    db_path = f"{pathlib.Path(workdir).as_posix()}/relay.db3"
    config_path = pathlib.Path(workdir) / "relay.toml"
    config_path.write_text(
        f'''
public_url = "{relay_url}"
listen = "127.0.0.1:{relay_port}"
db_path = "{db_path}"
default_hub = "http://127.0.0.1:{hub_port}/"
lease_seconds = 432000
renew_margin = "PT12H"
retention = "P7D"
manager_interval = "PT5M"
heartbeat = 30.0

[[tenants]]
id = "home"
token = "interop-token"
delivery_key = "interop-delivery-key"
''',
        encoding="utf-8",
    )

    log_path = pathlib.Path(workdir) / "relay.log"
    log_file = log_path.open("wb")
    proc = subprocess.Popen(
        [args.go_binary, "--config", str(config_path), "--log-level", "DEBUG"],
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    try:
        await wait_for_health(relay_url, proc)
        ws_url = f"ws://127.0.0.1:{relay_port}/v1/control"
        topic = "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc"

        async with ws_connect(ws_url) as websocket:
            await websocket.send(
                proto.encode_message(proto.Hello(tenant="home", token="interop-token"))
            )
            welcome = proto.decode_server_message(await websocket.recv())
            assert isinstance(welcome, proto.Welcome), welcome
            assert welcome.protocol_version == proto.PROTOCOL_VERSION

            await websocket.send(proto.encode_message(proto.Resume(cursor=0)))
            await websocket.send(
                proto.encode_message(
                    proto.WatchSync(
                        topics=[
                            proto.WatchTarget(
                                topic=topic,
                                routing={"kind": "youtube", "channel_id": "UCabc"},
                            )
                        ]
                    )
                )
            )
            state = proto.decode_server_message(await websocket.recv())
            assert isinstance(state, proto.WatchStateUpdate), state
            assert state.watches[0].topic == topics.normalize_topic(topic)
            assert state.watches[0].status == "active", state.watches[0]
            assert "callback" in hub_state

            body = b"<feed><yt:channelId>UCabc</yt:channelId></feed>"
            signature = "sha256=" + hmac.new(
                hub_state["secret"].encode(), body, hashlib.sha256
            ).hexdigest()
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    hub_state["callback"],
                    data=body,
                    headers={
                        "Content-Type": "application/atom+xml",
                        "X-Hub-Signature-256": signature,
                    },
                ) as response:
                    assert response.status == 204, response.status

            notification = proto.decode_server_message(await websocket.recv())
            assert isinstance(notification, proto.Notification), notification
            assert notification.routing == {"kind": "youtube", "channel_id": "UCabc"}
            assert notification.body == body.decode()
            assert notification.signature, "expected a delivery signature"
            assert proto.verify_delivery_signature(
                "interop-delivery-key",
                "home",
                notification.id,
                notification.topic,
                notification.body,
                notification.signature,
            )
            await websocket.send(proto.encode_message(proto.Ack(up_to=notification.id)))
            print("PASS: Python client <-> Go relay interop")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        log_file.close()
        await hub_runner.cleanup()
        if proc.returncode not in (0, None) and args.show_logs:
            print(log_path.read_text(encoding="utf-8", errors="replace"), file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-binary", default="websub-relayd")
    parser.add_argument("--relay-src", default=None)
    parser.add_argument("--show-logs", action="store_true")
    args = parser.parse_args()
    add_relay_src(args.relay_src)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
