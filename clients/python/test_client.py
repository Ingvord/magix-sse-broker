"""Integration tests: run against a live broker at MAGIX_URL (default http://localhost:8888)."""
import json
import os
import threading
import time

import httpx
import pytest

from magix_client import broadcast, subscribe

BASE_URL = os.environ.get("MAGIX_URL", "http://localhost:8888")


@pytest.fixture(autouse=True)
def require_broker():
    try:
        httpx.get(BASE_URL + "/", timeout=2).raise_for_status()
    except httpx.HTTPError:
        pytest.skip(f"no magix broker reachable at {BASE_URL}")


def collect(count, channels=()):
    """Run subscribe() in a thread; returns (thread, list that fills with events)."""
    events = []

    def run():
        for event in subscribe(BASE_URL, channels=channels, read_timeout=10):
            events.append(event)
            if len(events) >= count:
                return

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, events


def wait_until_subscribed():
    """The subscriber gauge is the broker's own view of how many clients are connected."""
    deadline = time.time() + 5
    while time.time() < deadline:
        value = httpx.get(BASE_URL + "/metrics/magix.subscribers.active", timeout=2).json()
        if value["measurements"][0]["value"] >= 1:
            return
        time.sleep(0.05)
    pytest.fail("subscriber never registered with the broker")


def test_subscriber_receives_three_broadcasts_and_exits():
    thread, events = collect(3, channels=["python-test"])
    wait_until_subscribed()

    ids = [broadcast(BASE_URL, {"n": n}, channel="python-test") for n in range(3)]
    thread.join(timeout=10)

    assert not thread.is_alive(), "subscriber did not finish after three broadcasts"
    assert [e.channel for e in events] == ["python-test"] * 3
    assert [json.loads(e.data) for e in events] == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert [e.id for e in events] == [str(i) for i in ids]


def test_connect_returns_without_any_event_being_published():
    """Regression for the 2022 hang: connecting to an idle broker must not block until an event."""
    started = time.time()
    with httpx.Client(timeout=httpx.Timeout(2.0)) as client:
        with client.stream("GET", BASE_URL + "/magix/api/subscribe") as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
    assert time.time() - started < 2.0


def test_envelope_opt_in():
    results = []
    thread = threading.Thread(
        target=lambda: results.extend(
            [next(iter(subscribe(BASE_URL, channels=["env"], envelope=True, read_timeout=10)))]
        ),
        daemon=True,
    )
    thread.start()
    wait_until_subscribed()
    message_id = broadcast(BASE_URL, {"k": "v"}, channel="env")
    thread.join(timeout=10)

    envelope = json.loads(results[0].data)
    assert envelope["id"] == message_id
    assert envelope["channel"] == "env"
    assert envelope["payload"] == {"k": "v"}
    assert "timestamp" in envelope
