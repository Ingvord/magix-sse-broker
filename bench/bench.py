#!/usr/bin/env python3
"""Measurements behind bench/measure.sh. Standard library only.

  startup  : spawn the broker N times; time from spawn until GET / first answers 200, and RSS 1 s later
  latency  : one broker, subscribers over raw sockets; broadcast-to-receive latency per delivered event

Every number printed here is measured by this file; nothing is estimated.
"""
import argparse
import asyncio
import http.client
import json
import math
import os
import re
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import time

HOST = "127.0.0.1"


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex((HOST, port)) == 0


def spawn(cmd, port):
    env = dict(os.environ, MICRONAUT_SERVER_PORT=str(port))
    return subprocess.Popen(shlex.split(cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            env=env, start_new_session=True)


def stop(proc, port):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    deadline = time.time() + 10
    while port_open(port) and time.time() < deadline:
        time.sleep(0.05)


def get_root_ok(port):
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=1)
        conn.request("GET", "/")
        response = conn.getresponse()
        response.read()
        conn.close()
        return response.status == 200
    except OSError:
        return False


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("VmRSS not found")


def start_and_wait(cmd, port, timeout=60):
    """Returns (process, milliseconds from just before spawn until GET / answered 200)."""
    if port_open(port):
        sys.exit(f"port {port} is already in use")
    started = time.perf_counter()
    proc = spawn(cmd, port)
    while True:
        if proc.poll() is not None:
            sys.exit(f"broker exited early (code {proc.returncode}): {cmd}")
        if get_root_ok(port):
            return proc, (time.perf_counter() - started) * 1000
        if time.perf_counter() - started > timeout:
            stop(proc, port)
            sys.exit(f"broker did not answer within {timeout}s: {cmd}")
        time.sleep(0.001)


def cmd_startup(args):
    # one discarded start so that executable files are in the page cache for every measured run
    proc, _ = start_and_wait(args.cmd, args.port)
    stop(proc, args.port)

    times, rss = [], []
    for _ in range(args.runs):
        proc, ms = start_and_wait(args.cmd, args.port)
        time.sleep(1.0)
        rss.append(rss_kb(proc.pid))
        stop(proc, args.port)
        times.append(ms)
    return {"startup_ms_median": statistics.median(times), "startup_ms_runs": [round(t, 1) for t in times],
            "rss_after_startup_kb_median": statistics.median(rss)}


def percentile(sorted_values, p):
    return sorted_values[min(len(sorted_values) - 1, math.ceil(p / 100 * len(sorted_values)) - 1)]


async def one_scenario(port, subscribers, messages, warmup):
    total = warmup + messages
    arrived = [0] * total
    done = [asyncio.Event() for _ in range(total)]
    samples = []
    pattern = re.compile(rb'data: \{"t":(\d+),"i":(\d+)\}\n')
    writers = []

    async def reader_task(reader, buf):
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                return
            buf += chunk
            end = 0
            for m in pattern.finditer(buf):
                now = time.monotonic_ns()
                i = int(m.group(2))
                if i >= warmup:
                    samples.append(now - int(m.group(1)))
                arrived[i] += 1
                if arrived[i] == subscribers:
                    done[i].set()
                end = m.end()
            buf = buf[end:]

    tasks = []
    for _ in range(subscribers):
        reader, writer = await asyncio.open_connection(HOST, port)
        writer.write(b"GET /magix/api/subscribe?channel=bench HTTP/1.1\r\nHost: bench\r\n"
                     b"Accept: text/event-stream\r\n\r\n")
        await writer.drain()
        buf = b""
        while b": keepalive" not in buf:  # the broker sends it once the subscription is registered
            buf += await reader.read(4096)
        writers.append(writer)
        tasks.append(asyncio.create_task(reader_task(reader, buf.split(b": keepalive", 1)[1])))

    post_reader, post_writer = await asyncio.open_connection(HOST, port)
    lost = 0
    for i in range(total):
        t0 = time.monotonic_ns()
        body = b'{"t":%d,"i":%d}' % (t0, i)
        post_writer.write(b"POST /magix/api/broadcast?channel=bench HTTP/1.1\r\nHost: bench\r\n"
                          b"Content-Type: application/json\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
        head = await post_reader.readuntil(b"\r\n\r\n")
        if b" 202 " not in head.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"broadcast answered: {head.split(chr(13).encode(), 1)[0]!r}")
        length = int(re.search(rb"(?i)content-length: (\d+)", head).group(1))
        await post_reader.readexactly(length)
        try:
            await asyncio.wait_for(done[i].wait(), 5)
        except asyncio.TimeoutError:
            lost += 1

    for t in tasks:
        t.cancel()
    for w in writers + [post_writer]:
        w.close()
    samples.sort()
    ms = lambda ns: round(ns / 1e6, 3)
    return {"subscribers": subscribers, "samples": len(samples), "lost_messages": lost,
            "p50_ms": ms(percentile(samples, 50)), "p99_ms": ms(percentile(samples, 99))}


def cmd_latency(args):
    proc, _ = start_and_wait(args.cmd, args.port)
    try:
        results = [asyncio.run(one_scenario(args.port, n, args.messages, args.warmup))
                   for n in args.subscribers]
        rss_after_load = rss_kb(proc.pid)
    finally:
        stop(proc, args.port)
    return {"latency": results, "rss_after_load_kb": rss_after_load,
            "messages_per_scenario": args.messages, "warmup_messages": args.warmup}


def sh(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def first_line(text):
    return text.splitlines()[0] if text else "n/a"


def hardware():
    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), "unknown")
    mem_kb = next(int(l.split()[1]) for l in open("/proc/meminfo") if l.startswith("MemTotal"))
    os_name = next((l.split("=", 1)[1].strip().strip('"') for l in open("/etc/os-release") if l.startswith("PRETTY_NAME")), "unknown")
    return f"{cpu}, {os.cpu_count()} logical CPUs, {mem_kb / 1024 / 1024:.0f} GiB RAM, {os_name}, kernel {os.uname().release}"


def native_image_bin():
    home = os.environ.get("GRAALVM_HOME")
    return os.path.join(home, "bin", "native-image") if home else "native-image"


def mb(n_bytes):
    return f"{n_bytes / 1_000_000:.1f} MB"


def docker_image_bytes(tag):
    out = sh("docker", "image", "inspect", tag, "--format", "{{.Size}}")
    return int(out) if out else None


def cmd_report(args):
    data = {name: json.load(open(path)) for name, path in (("JVM", args.jvm), ("Native", args.native)) if path}
    names = list(data)
    commit = sh("git", "rev-parse", "--short", "HEAD") or "unknown"
    dirty = " (with uncommitted changes)" if sh("git", "status", "--porcelain") else ""
    out = [f"# magix measurements, {time.strftime('%Y-%m-%d')}", "",
           f"- Commit: `{commit}`{dirty}",
           f"- Hardware: {hardware()}",
           f"- JVM: {first_line(subprocess.run(['java', '-version'], capture_output=True, text=True).stderr)}",
           f"- native-image (`GRAALVM_HOME` or PATH): {first_line(sh(native_image_bin(), '--version') or '')}",
           f"- Load average (1/5/15 min) when the run started: {args.loadavg}",
           f"- JVM command: `{args.jvm_cmd}`; native command: `{args.native_cmd}`",
           f"- Startup: {len(data[names[0]]['startup']['startup_ms_runs'])} fresh starts per target after one discarded start; "
           "time from just before spawn until `GET /` first returns 200; RSS read 1 s after that.",
           f"- Latency: per subscriber count, {data[names[0]]['latency']['warmup_messages']} warm-up broadcasts, then "
           f"{data[names[0]]['latency']['messages_per_scenario']} measured; a broadcast is sent only after the previous one reached every "
           "subscriber. Client and broker run on the same host and the client is a single-threaded Python process, "
           "so the numbers include client-side scheduling.", ""]

    out += ["## Startup and footprint", "", "| | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    row = lambda label, f: out.append(f"| {label} | " + " | ".join(f(data[n], n) for n in names) + " |")
    row("Time to first successful `GET /`, median", lambda d, n: f"{d['startup']['startup_ms_median']:.0f} ms")
    row("  all runs (ms)", lambda d, n: ", ".join(f"{x:.0f}" for x in d["startup"]["startup_ms_runs"]))
    row("RSS 1 s after startup, median", lambda d, n: f"{d['startup']['rss_after_startup_kb_median'] / 1024:.1f} MB")
    row("RSS after the latency runs", lambda d, n: f"{d['latency']['rss_after_load_kb'] / 1024:.1f} MB")
    artifacts = {"JVM": args.jvm_artifact, "Native": args.native_artifact}
    row("Executable / jar", lambda d, n: mb(os.path.getsize(artifacts[n])))
    images = {"JVM": "magix:jvm", "Native": "magix:native"}
    row("Docker image (`" + "`, `".join(images[n] for n in names) + "`)",
        lambda d, n: mb(docker_image_bytes(images[n])) if docker_image_bytes(images[n]) else "not built")

    out += ["", "## Broadcast-to-receive latency", "",
            "| Subscribers | " + " | ".join(f"{n} P50 | {n} P99" for n in names) + " | lost messages |",
            "|---|" + "---|---|" * len(names) + "---|"]
    for i, _ in enumerate(data[names[0]]["latency"]["latency"]):
        cells, lost = [], 0
        for n in names:
            r = data[n]["latency"]["latency"][i]
            cells += [f"{r['p50_ms']:.2f} ms", f"{r['p99_ms']:.2f} ms"]
            lost += r["lost_messages"]
        subs = data[names[0]]["latency"]["latency"][i]["subscribers"]
        out.append(f"| {subs} | " + " | ".join(cells) + f" | {lost} |")
    print("\n".join(out))


def cmd_measure(args):
    return {"startup": cmd_startup(args), "latency": cmd_latency(args)}


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("startup", "latency", "measure"):
        p = sub.add_parser(name)
        p.add_argument("--cmd", required=True)
        p.add_argument("--port", type=int, default=8890)
        if name in ("startup", "measure"):
            p.add_argument("--runs", type=int, default=10)
        if name in ("latency", "measure"):
            p.add_argument("--subscribers", type=lambda s: [int(x) for x in s.split(",")], default=[1, 10, 100])
            p.add_argument("--messages", type=int, default=500)
            p.add_argument("--warmup", type=int, default=200)
    rep = sub.add_parser("report")
    rep.add_argument("--jvm", required=True)
    rep.add_argument("--native")
    rep.add_argument("--loadavg", default="unknown")
    rep.add_argument("--jvm-cmd", required=True)
    rep.add_argument("--native-cmd", default="n/a")
    rep.add_argument("--jvm-artifact", required=True)
    rep.add_argument("--native-artifact")
    args = parser.parse_args()
    if args.command == "report":
        cmd_report(args)
        return
    print(json.dumps({"startup": cmd_startup, "latency": cmd_latency, "measure": cmd_measure}[args.command](args)))


if __name__ == "__main__":
    main()
