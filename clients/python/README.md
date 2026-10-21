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

The 2022 case study ended with a Python backend whose "SSEClient from sseclient package simply hangs on
connect()", while the earlier Tomcat/RESTEasy implementation worked. I did not have the original client
code, so I reproduced the behaviour against the broker as it was before this rework (commit `f22e89d`,
Micronaut 3.7.3, one shared `MulticastProcessor`), using the package the article names (`sseclient`
0.0.27, `check_sseclient.py`) and two other common clients (`requests` + `sseclient-py`, and
`httpx` + `httpx-sse`, `diagnose.py`). Three separate failure modes show up:

1. **No bytes until the first event.** On a freshly started broker the server did not send response
   headers until the first event was published. A client's connect call therefore blocks. With the
   article's package, `SSEClient(url)` did not return within 5 s; it returned only after something was
   published. `requests.get(stream=True)` and `httpx_sse.connect_sse` timed out after 3 s the same way. This
   matches "hangs on connect()".
2. **A compressed event stream.** HTTP libraries such as `requests` advertise `Accept-Encoding: gzip, deflate`
   by default, and Micronaut then gzipped the event stream (`Content-Encoding: gzip`). That happened on the
   old broker and on the first reworked version. `sseclient` reads the raw socket and does not decode the
   body, so it connects but never yields an event. `sseclient-py` and `httpx-sse` decode it and were
   unaffected, which is why this was found late. This gives a second way for that client to appear stuck.
3. **The stream dies when the buffer overflows.** After about 200 broadcasts with no subscriber, the old
   broker logged `MissingBackpressureException`. The first client to connect got the stale buffered
   events (124 in the run below) and then a protocol error (`ChunkedEncodingError`). The next client
   failed with `SSEError`, meaning the response was not an event stream. None of the three events published
   afterwards reached either client.

The reworked broker sends a keepalive comment as soon as the subscription is registered (and then every
`magix.heartbeat`), never compresses responses, keeps nothing for late subscribers, and gives each
subscriber its own drop-oldest buffer. In the same runs all three clients connect in well under 0.1 s and
receive all their events. Guards: `test_connect_returns_without_any_event_being_published` fails on the old
broker (`ReadTimeout`); `check_sseclient.py` (run by `run-tests.sh`, so also in CI) fails on the old broker and
whenever compression is re-enabled; the Java test `eventStreamIsNotCompressedEvenWhenClientAcceptsGzip`
covers the header.

`sseclient` 0.0.27 reports each keepalive comment as an event with empty data (it does not apply the SSE
rule that comments are ignored), so code that uses it should skip events with empty data.

Caveat: failure 1 matches the article's description most directly, and failures 2 and 3 are further
defects found on the way. None of this proves what the exact 2022 cause was: that setup used other
versions (the article gives no client version), and I did not measure whether a 2022-era `urllib3` would
have decoded the gzip body for `sseclient`, or whether Micronaut 4 with the old broker design would have
sent headers earlier.

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

The package named in the article (`sseclient` 0.0.27, requests 2.34.2, urllib3 2.8.0, Python 3.14.4),
from `check_sseclient.py`; the last two lines show what a client that advertises gzip is sent:

```
=== old (f22e89d), idle broker
FAIL: SSEClient() did not return within 5 s (blocked in connect)
exit=1
=== new (HEAD)
connected after 0.04 s
received 3/3 events ['{"n":0}', '{"n":1}', '{"n":2}']
OK
exit=0
--- Content-Encoding for a client that advertises gzip (requests default)
old (f22e89d):
  Content-Encoding: gzip
new:
  Content-Encoding: None
```
