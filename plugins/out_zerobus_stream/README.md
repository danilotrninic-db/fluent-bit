# zerobus_stream (POC)

Output plugin that streams log records into a Databricks Zerobus table over
long-lived gRPC `EphemeralStream` calls, using the pure C SDK from the
`effort/purec-poc-v2` zerobus-sdk worktree. It follows the design in
`~/docs/danilo-handoff.md`, section 2.

## How it works

- **No blocking in `cb_flush`.** A flush encodes the chunk, ingests it, and
  then suspends its coroutine until the server acknowledges the chunk's last
  offset. The worker keeps running other chunks in the meantime, so one worker
  has many chunks in flight. A chunk still returns `FLB_OK` only once it is
  durable, so Fluent Bit's delivery semantics are unchanged.
- **One poll timer per worker.** A 2 ms timer on each worker's event loop
  reads every waiting lane's acknowledged offset and resumes the chunks it
  covers. Acks are cumulative, so one ack can complete many chunks. The timer
  exists only while chunks are waiting.
- **Lanes.** `streams N` opens N SDK streams, each on its own connection
  (staging's 2 MiB HTTP/2 stream window caps a single stream from a distant
  host). All workers share the lanes. Each chunk goes whole to the lane with
  the fewest unacknowledged bytes. Ties go to the lowest lane, so light
  traffic keeps reusing a few warm connections: Linux restarts an idle TCP
  connection from a small congestion window (`tcp_slow_start_after_idle`).
  At shutdown all lanes close at once.
- **Failures reopen one lane.** The SDK reconnects and resends by itself.
  Once it gives up, only that lane goes down: its chunks return `FLB_RETRY`,
  and a per-lane opener thread reopens it with backoff after the last chunk
  lets go.
- **Bounded memory.** `buffer_size` caps the unacknowledged bytes per lane.
  A chunk that would exceed it waits (without blocking) for older requests
  to be acknowledged.
- **Encoding.** Each record body is encoded with `flb_msgpack_to_json` directly
  into one buffer per chunk, with no allocation per record, then split into
  requests of up to `batch_size`.

### SDK change this needs

The plugin needs a non-blocking "acked up to" query: suggestion 2 in the
handoff. purec-poc-v2 had only the blocking `zerobus_stream_flush`, so this
POC adds `zerobus_stream_get_acked_offset()` to that worktree, with a test.
The change is uncommitted there: `include/zerobus/stream.h`, `src/stream.c`,
`src/internal/ephemeral_stream/ephemeral_stream.{c,h}`,
`tests/src/stream_test.c` and `NEXT_CHANGELOG.md`.

## Build

The plugin is off by default and compiles the SDK from its checkout, against
Fluent Bit's bundled nghttp2 and protobuf-c runtime (no system copies needed):

```sh
cmake -S . -B build -DFLB_RELEASE=On -DFLB_DEBUG=Off -DFLB_CONFIG_YAML=Off \
  -DFLB_OUT_ZEROBUS_STREAM=On \
  -DFLB_ZEROBUS_SDK_PATH=$HOME/oss/zerobus-sdk-worktrees/effort/purec-poc-v2/purec
cmake --build build --target fluent-bit-bin -j16
```

Fluent Bit needs CMake 3.20 or newer (`~/.local/cmake-3.28.3/bin/cmake` here).
Build in release mode: debug builds are about 40% slower per worker.

## Options

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | required | Zerobus endpoint, `https://<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `unity_catalog_endpoint` | required | Workspace URL; the SDK mints table-scoped OAuth tokens there and refreshes them |
| `table_name` | required | `catalog.schema.table` |
| `client_id`, `client_secret` | required | Service principal; pass them as `${VAR}` |
| `ca_file` | system roots | PEM bundle to trust instead |
| `streams` | 1 | Lanes, each its own stream and connection (1 to 256) |
| `batch_size` | 4M | Largest ingest request; chunks are split to fit |
| `buffer_size` | 64M | Unacknowledged bytes per lane before chunks wait |
| `flush_timeout` | 30s | How long a chunk may wait to be sent and acknowledged before it is retried |
| `async_ack` | true | `false` blocks each flush in `zerobus_stream_flush`, for comparison only |
| `ack_poll_interval` | 2 | Milliseconds between polls while chunks wait |

`workers` defaults to 1 and must be at least 1.

### Throughput

Throughput is roughly the bytes in flight divided by the time a chunk takes
to be sent and acknowledged. From a host 120 ms from staging, one stream
carries about 9 MB/s (the 2 MiB stream window per round trip), so the rate
scales with `streams`, as long as there are enough chunks in flight to keep
every stream busy. Two settings outside the plugin decide that:

- **The input's `Mem_Buf_Limit`.** A chunk stays in its input's buffer until
  the output acknowledges it, so this limit caps the bytes in flight across
  all streams. Give it a few MB per stream: the demo uses 8 MB per stream.
- **Chunk size.** A chunk goes whole to one stream, so a few huge chunks keep
  only a few streams busy. Fluent Bit's usual chunks of about 2 MB are fine;
  an input that appends 10 MB at a time is not.

Measured on staging with 1 KiB records, two workers and enough input:
16 streams 141 MB/s, 64 streams 276 to 562 MB/s. The service varied by 2x
between runs minutes apart, while Fluent Bit used about 3 cores with no
thread saturated. The handoff's zbperf plugin measured 140 and 386 MB/s.

## Demo (dogfood staging)

`demo/zerobus_stream_demo.conf` sends dummy air quality records to
`shinkansen.default.air_quality_danilo` (`device_name STRING, temp INT,
humidity BIGINT`) from two generator inputs (one dummy input offers at most
about 400 MB/s). `demo/run_demo.sh` builds the record, 1 KiB of JSON by
default (the handoff's benchmark record size). `device_name` starts with the
run tag, which finds the run's rows, and carries the padding. It then offers
a fixed rate for a fixed time and measures the throughput:

```sh
source ~/.zshrc          # DATABRICKS_CLIENT_ID/SECRET, DATABRICKS_STAGING_TOKEN
plugins/out_zerobus_stream/demo/run_demo.sh
```

1. It starts Fluent Bit, waits for the streams to open, and lets a 5 s warmup
   pass without measuring it.
2. It measures 60 s and prints the acknowledged and input rates every 5 s.
   Only records the server acknowledged count (Fluent Bit's `proc_records`).
3. It prints the window's acknowledged, input and offered rates, and
   Fluent Bit's CPU: the whole process and its busiest thread. Then it names
   the limit. If the output kept up, the rate is only the load you offered:
   raise `DEMO_RATE`. If the inputs had to wait for the output and no thread
   is saturated, the network or the service is the limit.
4. It stops Fluent Bit, which drains the buffered chunks, and checks that the
   table has at least as many rows for the run as the plugin acknowledged.

Knobs, with their defaults: `DEMO_DURATION` (60), `DEMO_WARMUP` (5),
`DEMO_INTERVAL` (5), `DEMO_RATE` (1000000 records/s), `DEMO_RECORD_BYTES`
(1024), `DEMO_STREAMS` (16), `DEMO_WORKERS` (1), `DEMO_MEM_BUF_MB` (8 per
stream, at least 128), `DEMO_ASYNC_ACK` (on; off shows the blocking flush the
design avoids), and `DEMO_LOG_LEVEL` (info; debug logs every acknowledgment).
`DEMO_ENDPOINT`, `DEMO_UC_ENDPOINT` and `DEMO_TABLE` point it elsewhere.
Fluent Bit's log goes to `/tmp/zerobus_stream_<run>.log`.

The defaults offer about 1 GB/s, more than staging takes, so a run writes
whatever staging accepts. That's about 140 MB/s with the default 16 streams,
around 9 GB per 60 s run; with 64 streams it's 15 to 35 GB. The table's owner
must grant the ingesting service principal `SELECT` and `MODIFY` on it.

## Delivery and limits

- At least once. A retried chunk is ingested again, so rows committed before
  a failure or timeout are duplicated.
- If the SDK rejects a request's records (`INVALID_ARGUMENT`, such as invalid
  UTF-8), the chunk is dropped with `FLB_ERROR`. Any earlier requests from the
  same chunk have already been sent.
- A record that cannot fit in one request (10 MiB - 64 KiB) is dropped and
  logged. The rest of its chunk is sent.
- Every lane mints its own OAuth token.
- Shutdown waits for in-flight chunks to be acknowledged, up to `flush_timeout`.
- Linux only, like the SDK's platform layer. No unit or runtime tests yet.
