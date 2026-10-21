# magix measurements, 2026-10-21

- Commit: `184f89d`
- Hardware: Intel(R) Core(TM) Ultra 7 265, 20 logical CPUs, 62 GiB RAM, Ubuntu 26.04.1 LTS, kernel 7.0.0-38-generic
- JVM: openjdk version "21.0.2" 2024-01-16
- native-image (`GRAALVM_HOME` or PATH): native-image 21 2023-09-19
- Load average (1/5/15 min) when the run started: 3.99 4.85 3.90
- JVM command: `java -jar target/magix-0.1.jar`; native command: `target/magix`
- Startup: 10 fresh starts per target after one discarded start; time from just before spawn until `GET /` first returns 200; RSS read 1 s after that.
- Latency: per subscriber count, 200 warm-up broadcasts, then 500 measured; a broadcast is sent only after the previous one reached every subscriber. Client and broker run on the same host and the client is a single-threaded Python process, so the numbers include client-side scheduling.

## Startup and footprint

| | JVM | Native |
|---|---|---|
| Time to first successful `GET /`, median | 662 ms | 47 ms |
|   all runs (ms) | 665, 665, 638, 668, 648, 666, 659, 672, 653, 626 | 45, 49, 52, 49, 51, 43, 44, 37, 38, 50 |
| RSS 1 s after startup, median | 192.6 MB | 75.8 MB |
| RSS after the latency runs | 333.4 MB | 135.9 MB |
| Executable / jar | 14.7 MB | 80.9 MB |
| Docker image (`magix:jvm`, `magix:native`) | 334.2 MB | 97.6 MB |

## Broadcast-to-receive latency

| Subscribers | JVM P50 | JVM P99 | Native P50 | Native P99 | lost messages |
|---|---|---|---|---|---|
| 1 | 0.26 ms | 0.70 ms | 0.07 ms | 0.29 ms | 0 |
| 10 | 0.19 ms | 0.53 ms | 0.15 ms | 0.60 ms | 0 |
| 100 | 0.66 ms | 1.56 ms | 0.75 ms | 2.76 ms | 0 |
