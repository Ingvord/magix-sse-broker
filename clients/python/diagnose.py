"""Probe how common Python SSE clients behave against a magix broker.

    pip install -r requirements-diagnose.txt
    python diagnose.py http://localhost:8888 [--prefill 200]

--prefill N publishes N events before any client connects (do this against a freshly started broker).
Used to reproduce the 2022 hang; see README.md in this directory for the results.
"""
import argparse
import threading
import time

import httpx
import requests
import sseclient
from httpx_sse import connect_sse

CHANNEL = "probe"


def post(base, channel, body):
    return requests.post(f"{base}/magix/api/broadcast", params={"channel": channel}, json=body, timeout=5).status_code


def headers_requests(base):
    started = time.time()
    try:
        r = requests.get(f"{base}/magix/api/subscribe", stream=True, timeout=(3, 3))
        return f"headers after {time.time() - started:.2f}s (HTTP {r.status_code})"
    except requests.RequestException as e:
        return f"NO HEADERS after {time.time() - started:.2f}s ({type(e).__name__})"


def headers_httpx(base):
    started = time.time()
    try:
        with httpx.Client(timeout=httpx.Timeout(3.0)) as client:
            with connect_sse(client, "GET", f"{base}/magix/api/subscribe") as source:
                return f"connected after {time.time() - started:.2f}s (HTTP {source.response.status_code})"
    except httpx.HTTPError as e:
        return f"NO CONNECT after {time.time() - started:.2f}s ({type(e).__name__})"


def events_sseclient(base):
    r = requests.get(f"{base}/magix/api/subscribe", stream=True, timeout=(3, 6))
    for e in sseclient.SSEClient(r).events():
        yield e.event, e.data


def events_httpx(base):
    with httpx.Client(timeout=httpx.Timeout(3.0, read=6.0)) as http:
        with connect_sse(http, "GET", f"{base}/magix/api/subscribe") as source:
            for s in source.iter_sse():
                yield s.event, s.data


def receive(base, events_from, count=3):
    """Subscribe, publish `count` events, report how many of them arrive and what else shows up."""
    got, error = [], []

    def run():
        try:
            for event in events_from(base):
                got.append(event)
                if sum(1 for name, _ in got if name == CHANNEL) >= count:
                    return
        except Exception as e:  # report whatever ended the stream
            error.append(type(e).__name__)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(1.0)
    statuses = {post(base, CHANNEL, {"n": n}) for n in range(count)}
    thread.join(8)
    own = sum(1 for name, _ in got if name == CHANNEL)
    return f"POST {sorted(statuses)}, received {own}/{count} of ours, {len(got) - own} stale, ended with {error or 'ok'}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url")
    parser.add_argument("--prefill", type=int, default=0)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    for n in range(args.prefill):
        post(base, "early", {"n": n})
    if args.prefill:
        print(f"published {args.prefill} events before any subscriber")
    print("requests  :", headers_requests(base))
    print("httpx-sse :", headers_httpx(base))
    print("sseclient-py:", receive(base, events_sseclient))
    print("httpx-sse   :", receive(base, events_httpx))
