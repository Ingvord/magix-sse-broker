# Python client

`magix_client.py` is a small client on top of `httpx` and `httpx-sse`:

```python
from magix_client import broadcast, subscribe

for event in subscribe("http://localhost:8888", channels=["sensors"]):
    print(event.channel, event.id, event.data)

message_id = broadcast("http://localhost:8888", {"temp": 21.5}, channel="sensors")
```

Tests (`test_client.py`) subscribe, receive three broadcasts and exit. They need a running broker:

```bash
pip install -r requirements-dev.txt
./run-tests.sh [path/to/magix.jar]   # starts the jar on port 8889, runs pytest, stops it
# or: MAGIX_URL=http://localhost:8888 pytest
```

## What the 2022 hang turned out to be

The 2022 case study ended with a hang between the SSE endpoint and a Python SSE client. I did not have
the original client code, so I reproduced the behaviour with two common clients (`requests` +
`sseclient-py`, and `httpx` + `httpx-sse`) against the broker as it was before this rework (commit
`f22e89d`, Micronaut 3.7.3, one shared `MulticastProcessor`). `diagnose.py` repeats the measurements
against any broker. Two separate failure modes show up:

1. **No bytes until the first event.** On a freshly started broker the server did not send response
   headers until the first event was published. A client's `connect` call (`requests.get(stream=True)`,
   `httpx_sse.connect_sse`) therefore blocks, and any client that waits for the response before it does
   anything else looks hung. Both clients timed out after 3 s with nothing received. Once the first event
   was published, headers and events arrived normally.
2. **The stream dies after 128 unobserved events.** After about 200 broadcasts with no subscriber, the old
   broker logged `MissingBackpressureException`. The first client to connect got the stale buffered
   events (124 in the run below) and then a protocol error (`ChunkedEncodingError`). The next client
   failed with `SSEError`, meaning the response was not an event stream. None of the three events published
   afterwards reached either client.

The reworked broker sends a keepalive comment as soon as the subscription is registered (and then every
`magix.heartbeat`), keeps nothing for late subscribers, and gives each subscriber its own drop-oldest
buffer. In the same runs both clients connect in about 0.04 s and receive all three events, with and
without the 200-event prefill. `test_connect_returns_without_any_event_being_published` fails on the old
broker (`ReadTimeout`) and passes on the new one.

Caveat: this shows two defects that match the reported symptom, not that they were the exact cause in
2022. That setup used different versions, and I did not measure the 2022 client. I also did not measure
whether Micronaut 4 with the old broker design would have sent headers earlier.

### Captured run

Python 3.14.4, httpx 0.28.1, httpx-sse 0.4.3, requests 2.34.2, sseclient-py 1.9.0, JDK 21.0.2. "Old" is
commit `f22e89d`, "new" is this branch. Broker started fresh for each block.

```
=== old (f22e89d), prefill 0
requests  : NO HEADERS after 3.00s (ReadTimeout)
httpx-sse : NO CONNECT after 3.02s (ReadTimeout)
sseclient-py: POST [200], received 3/3 of ours, 0 stale, ended with ok
httpx-sse   : POST [200], received 3/3 of ours, 0 stale, ended with ok
=== old (f22e89d), prefill 200
published 200 events before any subscriber
requests  : headers after 0.02s (HTTP 200)
httpx-sse : connected after 0.02s (HTTP 200)
sseclient-py: POST [200], received 0/3 of ours, 124 stale, ended with ['ChunkedEncodingError']
httpx-sse   : POST [200], received 0/3 of ours, 0 stale, ended with ['SSEError']
=== new, prefill 0
requests  : headers after 0.04s (HTTP 200)
httpx-sse : connected after 0.02s (HTTP 200)
sseclient-py: POST [202], received 3/3 of ours, 0 stale, ended with ok
httpx-sse   : POST [202], received 3/3 of ours, 0 stale, ended with ok
=== new, prefill 200
published 200 events before any subscriber
requests  : headers after 0.04s (HTTP 200)
httpx-sse : connected after 0.02s (HTTP 200)
sseclient-py: POST [202], received 3/3 of ours, 0 stale, ended with ok
httpx-sse   : POST [202], received 3/3 of ours, 0 stale, ended with ok
```
