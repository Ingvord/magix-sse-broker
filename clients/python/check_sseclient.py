"""Check that the PyPI package "sseclient" can read a magix stream.

This is the client named in the 2022 case study. It is a different package from "sseclient-py" (used
by diagnose.py), but both install a module called "sseclient", so install this one in its own
virtualenv:

    python3 -m venv /tmp/sseclient-venv
    /tmp/sseclient-venv/bin/pip install -r requirements-sseclient.txt
    /tmp/sseclient-venv/bin/python check_sseclient.py http://localhost:8888

It reads the raw socket, so it needs the server to answer without compression (it does not decode a
gzip body, which `requests` asks for by default) and without blocking before the first event.
Note: sseclient 0.0.27 reports each keepalive comment as an event with empty data (the SSE rule that
comments are ignored is not applied), so application code using it should skip events with empty data.
Exit status 0 means it connected within 5 s and received three broadcasts.
"""
import sys
import threading
import time

import requests
from sseclient import SSEClient

CHANNEL = "sseclient-check"
CONNECT_TIMEOUT_S = 5
EVENTS_TIMEOUT_S = 5


def main(base):
    state = {"connected_after_s": None, "events": [], "error": None}
    started = time.time()

    def run():
        try:
            client = SSEClient(f"{base}/magix/api/subscribe?channel={CHANNEL}")  # connects here
            state["connected_after_s"] = time.time() - started
            for event in client:
                if not event.data:  # this client reports each keepalive comment as an empty event
                    continue
                state["events"].append(event.data)
                if len(state["events"]) == 3:
                    return
        except Exception as e:
            state["error"] = repr(e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    while state["connected_after_s"] is None and time.time() - started < CONNECT_TIMEOUT_S and thread.is_alive():
        time.sleep(0.01)
    if state["connected_after_s"] is None:
        print(f"FAIL: SSEClient() did not return within {CONNECT_TIMEOUT_S} s (blocked in connect)")
        return 1
    print(f"connected after {state['connected_after_s']:.2f} s")

    for n in range(3):
        requests.post(f"{base}/magix/api/broadcast", params={"channel": CHANNEL}, json={"n": n}, timeout=5).raise_for_status()
    thread.join(EVENTS_TIMEOUT_S)
    print(f"received {len(state['events'])}/3 events {state['events']}" + (f", error {state['error']}" if state["error"] else ""))
    if len(state["events"]) != 3:
        print("FAIL: connected but did not receive the broadcasts")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8888"))
