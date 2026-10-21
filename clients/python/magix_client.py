"""Minimal Python client for a magix broker."""
from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

import httpx
from httpx_sse import connect_sse


@dataclass(frozen=True)
class Event:
    channel: str
    id: Optional[str]
    data: str


def broadcast(base_url: str, payload, channel: str = "message", timeout: float = 5.0) -> int:
    """Publish a JSON-serialisable payload and return the id the broker assigned to it."""
    response = httpx.post(
        f"{base_url}/magix/api/broadcast",
        params={"channel": channel},
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()["id"]


def subscribe(
    base_url: str,
    channels: Sequence[str] = (),
    envelope: bool = False,
    read_timeout: Optional[float] = None,
) -> Iterator[Event]:
    """Yield events as they arrive. No channels means every channel.

    The generator holds the connection open until it is closed or the broker goes away. Keepalive
    comments are consumed by the SSE parser and never surface as events.
    """
    params = {}
    if channels:
        params["channel"] = ",".join(channels)
    if envelope:
        params["envelope"] = "true"
    timeout = httpx.Timeout(5.0, read=read_timeout)
    with httpx.Client(timeout=timeout) as client:
        with connect_sse(client, "GET", f"{base_url}/magix/api/subscribe", params=params) as source:
            for sse in source.iter_sse():
                yield Event(channel=sse.event, id=sse.id or None, data=sse.data)
