# magix

A small server-sent-events (SSE) message broker. As a native image it answers its first request 47 ms after process start and ships as a 97.6 MB Docker image (measured, [details below](#measured-results)).

Clients `POST` a JSON message to a named channel, and every connected subscriber receives it as an SSE event. It is built on Micronaut 4 and Project Reactor and runs on the JVM or as a GraalVM native image. It needs no database or external service.

It started as the messaging layer for a distributed control platform; a [2022 case study](https://medium.com/@ingvord.mail/micronaut-graalvm-server-sent-events-based-message-broker-a-case-study-be6e67c16abe) describes the first native-image version. This repository is that broker, reworked: upgraded to current Micronaut and Java, with its known defects fixed, tested, and measured.

## What it does

- `POST /magix/api/broadcast?channel=<name>` publishes a JSON value to a channel.
- `GET /magix/api/subscribe` streams every published message as an SSE event named after its channel. `?channel=a,b` restricts a subscription to some channels.
- It keeps nothing: no history, no replay. A subscriber sees what is published while it is connected.

**Why a native image?** A broker like this is small, long-lived, and often one of many containers on a node, so startup time and resident memory matter more than peak throughput. In the measurements below the native build starts about 14 times faster and uses well under half the memory of the JVM build. Delivery latency is in the same range for both, so speed after startup is not the argument.

## Quickstart

```bash
./mvnw -B -DskipTests package                 # JDK 21
java -jar target/magix-0.1.jar &              # listens on :8888

curl -N 'localhost:8888/magix/api/subscribe?channel=sensors' &
curl -X POST -H 'Content-Type: application/json' -d '{"temp":21.5}' \
     'localhost:8888/magix/api/broadcast?channel=sensors'     # {"id":1}

# or in a container: docker build -t magix:native . && docker run -p 8888:8888 magix:native
```

The subscriber prints a keepalive comment, then:

```
event: sensors
id: 1
data: {"temp":21.5}
```

More ways to run it: `docker compose -f deploy/compose.yaml up --build`, or `kubectl apply -f deploy/k8s.yaml` (build the image and load it into your cluster first; see the header of that file). A native executable needs GraalVM CE 21: `./mvnw -B -Dpackaging=native-image -DskipTests package` produces `target/magix`.

## API

### `POST /magix/api/broadcast?channel=<name>`

| | |
|---|---|
| Query `channel` | Channel name, default `message`. Must not contain line breaks (it becomes the SSE event name). |
| Body | Any JSON value. `Content-Type: application/json` is required. |
| `202 Accepted` | `{"id": 42}`: the id assigned to the message. The broker is fire-and-forget, so this means "accepted", not "delivered". |
| `400` | Invalid or empty JSON, or a channel name containing a line break. |
| `415` | The content type is not JSON. |

### `GET /magix/api/subscribe`

| | |
|---|---|
| Query `channel` | Comma-separated channel names, e.g. `a,b`. Omitted or blank means every channel. |
| Query `envelope` | `true` wraps each message in the envelope below. Default `false`. |
| Response | `200`, `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`. The first bytes arrive as soon as the subscription is registered. |

Each message becomes one SSE event:

```
event: <channel>
id: <message id>
data: <the posted JSON, on one line>
```

Idle subscriptions receive a comment line (`: keepalive`) every `magix.heartbeat`. SSE clients ignore comments, so they never surface as events.

**Message envelope** (only with `?envelope=true`; otherwise `data` is the raw payload):

| Field | Type | Meaning |
|---|---|---|
| `id` | number | Assigned on publish, increasing, starting at 1 for each process start. |
| `channel` | string | The channel it was published to. |
| `timestamp` | string | ISO-8601 UTC instant of publication. |
| `payload` | any JSON | Exactly what was posted. |

### Other endpoints

- `GET /` returns `OK!`.
- `GET /health` returns `{"status":"UP"}`.
- `GET /metrics/{name}` exposes `magix.subscribers.active` (gauge), `magix.events.published` and `magix.events.dropped` (counters).

### Configuration

| Property | Environment variable | Default | Meaning |
|---|---|---|---|
| `magix.buffer-size` | `MAGIX_BUFFER_SIZE` | `256` | Events buffered per subscriber before its oldest are dropped. |
| `magix.heartbeat` | `MAGIX_HEARTBEAT` | `15s` | Interval between keepalive comments. |
| `micronaut.server.port` | `MICRONAUT_SERVER_PORT` | `8888` | Listen port. |

## Design notes

**Backpressure: drop oldest, per subscriber.** The broker is a multicast sink that retains nothing, and each subscriber gets its own bounded buffer in front of its connection. When a client reads too slowly and its buffer fills, its oldest events are discarded and `magix.events.dropped` is incremented. A slow or stalled client therefore only hurts itself; publishers and other subscribers are never held up. The cost is that slow clients lose messages. On an unfiltered subscription, a gap in the event ids shows that this happened.

**Heartbeat.** Proxies and load balancers close idle connections, and some clients do not report "connected" until the first byte arrives. The server sends a comment frame as soon as the subscription is registered and then every `magix.heartbeat`. Because it is sent after registration, a client that has seen it will not miss anything published afterwards.

**Channel filtering.** The filter is applied before the per-subscriber buffer, so events a client did not ask for never take up its buffer space.

**No response compression.** Event frames are tiny, and some SSE clients read the raw socket and cannot decode a gzip body, which HTTP libraries request by default. Responses are never compressed (`micronaut.server.netty.compression-threshold: -1`).

**Hand-written SSE frames.** Micronaut's `Event` encoder always writes a `data:` line, so a comment-only keepalive would come out as `data: null`, which clients dispatch as a real event. The broker serialises the few frame types itself (`SseFrames`). Channel names with line breaks are rejected because they could otherwise forge frames.

## Measured results

Produced by [`bench/measure.sh`](bench/measure.sh); the committed output is [`bench/results/2026-10-21-intel-core-ultra-7-265.md`](bench/results/2026-10-21-intel-core-ultra-7-265.md), and CI publishes a fresh report as the job summary of its `native` job.

Hardware: Intel Core Ultra 7 265, 20 logical CPUs, 62 GiB RAM, Ubuntu 26.04.1, kernel 7.0.0-38-generic. JVM: OpenJDK 21.0.2. Native: GraalVM CE 21+35. Commit `184f89d`. The machine was shared (load average about 4 at the start of the run).

| | JVM | Native |
|---|---|---|
| Time to first successful `GET /`, median of 10 starts | 662 ms | 47 ms |
| RSS 1 s after startup | 192.6 MB | 75.8 MB |
| RSS after the latency runs | 333.4 MB | 135.9 MB |
| Executable / jar | 14.7 MB | 80.9 MB |
| Docker image | 334.2 MB | 97.6 MB |

Broadcast-to-receive latency, P50 / P99 (200 warm-up broadcasts, then 500 measured; no messages lost):

| Subscribers | JVM | Native |
|---|---|---|
| 1 | 0.26 / 0.70 ms | 0.07 / 0.29 ms |
| 10 | 0.19 / 0.53 ms | 0.15 / 0.60 ms |
| 100 | 0.66 / 1.56 ms | 0.75 / 2.76 ms |

How to read these: startup is measured from just before the process is spawned until `GET /` first answers 200. The load generator is a single-threaded Python process on the same host, so latencies include client-side scheduling, and the P99 values are indicative only (they varied between repeated runs on this shared machine). The JVM image is large because it uses the full `eclipse-temurin:21-jre` base.

The 2022 case study reported about 18 ms startup and a 55.92 MB native image (the executable) for the original version. This build measures 47 ms by the definition above and an 80.9 MB executable (97.6 MB Docker image); the size difference has not been investigated.

## The 2022 Python client hang

The case study ended with a Python client that "simply hangs on connect()". Reproducing it with the package the article names (`sseclient`) and two other Python clients against the pre-rework broker showed three defects:

1. **No bytes until the first event.** The old server sent no response headers until something was published, so a client's connect call blocked on an idle broker. This matches the article's description.
2. **A gzipped event stream.** `requests` advertises gzip by default and the server compressed the stream. `sseclient` reads the raw socket and cannot decode that, so it connected but never saw an event.
3. **The stream died when the buffer overflowed.** After about 200 broadcasts with nobody subscribed, the single shared buffer failed with a backpressure error, and later events did not reach the clients tried.

All three are fixed (immediate keepalive, no response compression, per-subscriber buffers) and guarded by tests that run in CI. This establishes three real defects, the first matching the article's symptom directly. It does not prove the exact 2022 cause, because that setup used other versions. The reproduction scripts, captured output, and caveats are in [`clients/python/`](clients/python/README.md).

## Development

```bash
./mvnw -B verify                                  # unit and integration tests (JDK 21)
clients/python/run-tests.sh                       # Python clients (incl. the article's sseclient) against the packaged jar
bench/measure.sh --out report.md                  # needs the native executable, the jar and optionally the Docker images
```

The tests use only the JDK's HTTP client, so they exercise the broker the way `curl` or a browser would.

## Known limitations

- **No authentication or TLS.** Anyone who can reach the port can publish, subscribe, and read `/metrics`. Put it behind a reverse proxy or a network policy.
- **No persistence and no replay.** A subscriber only sees messages published while it is connected. `Last-Event-ID` is ignored, and ids restart at 1 when the process restarts.
- **Single node.** State is in memory. Running several replicas gives several independent brokers.
- **At-most-once delivery.** Slow subscribers lose their oldest events (see above), and a `202` does not confirm delivery.
- **Measurements come from one machine.** Treat them as a worked example of the method, not as a benchmark of your environment.

## License

[MIT](LICENSE)

## Links

- [Case study (2022)](https://medium.com/@ingvord.mail/micronaut-graalvm-server-sent-events-based-message-broker-a-case-study-be6e67c16abe): the first native-image version and the hang.
- [`scientific-software-hub/magix`](https://github.com/scientific-software-hub/magix): meta repository with the RFCs and other implementations.
