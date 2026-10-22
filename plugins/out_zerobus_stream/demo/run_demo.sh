#!/usr/bin/env bash
#
# zerobus_stream demo: offer a fixed rate of 1 KiB records for a fixed time,
# measure the acknowledged throughput, then check that the rows landed.
#
# Timeline: Fluent Bit starts and opens its streams, DEMO_WARMUP seconds pass
# unmeasured, then DEMO_DURATION seconds are measured with a progress line
# every DEMO_INTERVAL seconds. Fluent Bit then stops: the input stops and the
# buffered chunks drain. Throughput counts only records whose chunk the server
# acknowledged (Fluent Bit's proc_records), within the measured window. MB is
# 10^6 bytes of JSON payload.
#
# Needs DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (the service
# principal that ingests). The row count also needs DATABRICKS_STAGING_TOKEN,
# a PAT for the SQL API; without it that step is skipped. Secrets are read
# from the environment only and never printed.
#
# Knobs, all optional:
#   DEMO_DURATION   measured seconds (default 60)
#   DEMO_WARMUP     unmeasured seconds before that (default 5)
#   DEMO_INTERVAL   seconds between progress lines (default 5)
#   DEMO_RATE       offered records per second, split between two generator
#                   inputs (default 1000000)
#   DEMO_RECORD_BYTES  size of each record's JSON (default 1024)
#   DEMO_STREAMS    streams, one connection each (default 16)
#   DEMO_WORKERS    output workers, which share the streams (default 1)
#   DEMO_MEM_BUF_MB input buffer in MB, split between the inputs. It also
#                   bounds the bytes in flight, since a chunk stays buffered
#                   until it is acknowledged (default 8 per stream, at least
#                   128)
#   DEMO_ASYNC_ACK  on or off (default on); off blocks each flush, for contrast
#   DEMO_LOG_LEVEL  plugin log level (default info; debug logs every ack)
#   DEMO_RUN        tag that starts each row's device_name (default demo-<time>)
#   DEMO_ENDPOINT, DEMO_UC_ENDPOINT, DEMO_TABLE, DEMO_WAREHOUSE
#                   target and SQL warehouse (default: dogfood staging)
#   FLB             fluent-bit binary (default <repo>/build/bin/fluent-bit)
# Fluent Bit's own log goes to /tmp/zerobus_stream_<run>.log.

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)

DURATION=${DEMO_DURATION:-60}
WARMUP=${DEMO_WARMUP:-5}
INTERVAL=${DEMO_INTERVAL:-5}
RATE=${DEMO_RATE:-1000000}
export DEMO_STREAMS=${DEMO_STREAMS:-16}
export DEMO_WORKERS=${DEMO_WORKERS:-1}
# Every stream needs a few chunks in flight to stay busy, and the input's
# buffer holds them until they are acknowledged: it must grow with streams.
MEM_BUF_MB=${DEMO_MEM_BUF_MB:-$(( DEMO_STREAMS * 8 > 128 ? DEMO_STREAMS * 8 : 128 ))}
export DEMO_INPUT_BUF_LIMIT=$(( MEM_BUF_MB / 2 ))M
export DEMO_ASYNC_ACK=${DEMO_ASYNC_ACK:-on}
export DEMO_LOG_LEVEL=${DEMO_LOG_LEVEL:-info}
export DEMO_RUN=${DEMO_RUN:-demo-$(date -u +%Y%m%d-%H%M%S)}
export DEMO_HTTP_PORT=${DEMO_HTTP_PORT:-2021}
export DEMO_ENDPOINT=${DEMO_ENDPOINT:-https://6051921418418893.zerobus.us-west-2.staging.cloud.databricks.com}
export DEMO_UC_ENDPOINT=${DEMO_UC_ENDPOINT:-https://e2-dogfood.staging.cloud.databricks.com}
export DEMO_TABLE=${DEMO_TABLE:-shinkansen.default.air_quality_danilo}
WAREHOUSE=${DEMO_WAREHOUSE:-dd43ee29fedd958d}
FLB=${FLB:-$repo/build/bin/fluent-bit}
LOG=/tmp/zerobus_stream_$DEMO_RUN.log

: "${DATABRICKS_CLIENT_ID:?set DATABRICKS_CLIENT_ID (source ~/.zshrc)}"
: "${DATABRICKS_CLIENT_SECRET:?set DATABRICKS_CLIENT_SECRET (source ~/.zshrc)}"
[ -x "$FLB" ] || { echo "no fluent-bit binary at $FLB" >&2; exit 1; }
[ "$RATE" -ge 2 ] || { echo "DEMO_RATE must be at least 2" >&2; exit 1; }

# Two generators, as one dummy input offers at most about 400 MB/s. Each
# ticks up to 1000 times a second, with as many records per tick as its half
# of the rate needs: at most 500 records (0.5 MB) up to 1M records/s. A tick
# is one append, so bigger ticks make bigger chunks, and a chunk goes whole
# to one stream. RATE becomes what is actually offered.
half=$(( RATE / 2 ))
export DEMO_TICKS=$(( half < 1000 ? half : 1000 ))
export DEMO_COPIES=$(( half / DEMO_TICKS ))
RATE=$(( 2 * DEMO_TICKS * DEMO_COPIES ))
# An air quality record with the table's columns: device_name, temp and
# humidity. device_name, the only string, starts with the run tag, which finds
# this run's rows, and is padded so the JSON the plugin sends is exactly
# RECORD_BYTES long.
RECORD_BYTES=${DEMO_RECORD_BYTES:-1024}
DEMO_RECORD=$(python3 -I - "$DEMO_RUN" "$RECORD_BYTES" <<'EOF'
import json, sys

run, size = sys.argv[1], int(sys.argv[2])
record = {"device_name": run + "/", "temp": 21, "humidity": 45}
pad = size - len(json.dumps(record, separators=(",", ":")))
if pad < 0:
    sys.exit("DEMO_RECORD_BYTES is too small for the record's other fields")
text = "sensor padded to a %d-byte record: " % size
text += "lorem ipsum dolor sit amet consectetur adipiscing elit " * (pad // 50 + 1)
record["device_name"] += text[:pad]
print(json.dumps(record, separators=(",", ":")))
EOF
)
export DEMO_RECORD

now() { date +%s.%N; }

# ---- display ----------------------------------------------------------

# fmt count N         N with thousands separators
# fmt rps N S         N records over S seconds, as records/s
# fmt mbps N S        N records over S seconds, as MB/s
# fmt secs S          S seconds, to a tenth
fmt() {
    awk -v what="$1" -v n="$2" -v s="${3:-1}" -v b="$RECORD_BYTES" '
        function group(x,   out) {
            out = ""
            while (length(x) > 3) {
                out = "," substr(x, length(x) - 2) out
                x = substr(x, 1, length(x) - 3)
            }
            return x out
        }
        BEGIN {
            if (what == "count") {
                printf "%s", group(sprintf("%.0f", n))
            }
            else if (what == "rps") {
                printf "%s", group(sprintf("%.0f", n / s))
            }
            else if (what == "mbps") {
                split(sprintf("%.1f", n * b / s / 1e6), part, ".")
                printf "%s.%s", group(part[1]), part[2]
            }
            else if (what == "secs") {
                printf "%.1f", n
            }
        }'
}

# What a Fluent Bit thread does, from its name (cut to 15 characters)
thread_role() {
    local name=${1%-}
    case "$name" in
        flb-in-*)     echo "generator input $name" ;;
        flb-out-*)    echo "output worker $name" ;;
        flb-pipeline) echo "engine thread" ;;
        fluent-bit)   echo "SDK I/O or main thread" ;;
        *)            echo "thread $name" ;;
    esac
}

# "label   value" lines, aligned under each section
row() {
    local label=$1
    shift
    printf "  %-14s %s\n" "$label" "$*"
}

# Plugin log lines without the date and the plugin prefix
plugin_lines() {
    sed -E 's/^\[[0-9\/]+ ([0-9:]+)\.[0-9]+\] \[ *([a-z]+)\] \[output:[^]]*\] /\1 \2: /'
}

# ---- Fluent Bit's counters --------------------------------------------

# One sample: "<time> <records in> <acknowledged>". Fails while the HTTP
# server is not up yet.
metrics="http://127.0.0.1:$DEMO_HTTP_PORT/api/v1/metrics/prometheus"
sample() {
    local t body
    t=$(now)
    body=$(curl -s --max-time 2 "$metrics" 2>/dev/null || true)
    awk -v t="$t" '
        /^fluentbit_input_records_total\{name="dummy\.[0-9]+"\}/ { input += $2 }
        /^fluentbit_output_proc_records_total\{name="zerobus_stream.0"\}/ { acked = $2 }
        END { if (acked == "") exit 1; printf "%s %.0f %.0f\n", t, input, acked }' \
        <<< "$body"
}

# How often an input has paused so far because its buffer was full, that is,
# waited for the output to acknowledge chunks
pauses() { grep -c "paused (mem buf overlimit" "$LOG" || true; }

# CPU clock ticks Fluent Bit used so far, process-wide (exited threads
# included), and per thread as "<tid> <ticks> <name>" lines. The fields
# after the parenthesized name start at the state, so utime and stime are
# the 12th and 13th.
proc_ticks() {
    awk '{ sub(/^.*\) /, ""); print $12 + $13 }' /proc/"$flb_pid"/stat
}
thread_ticks() {
    awk '{ name = $0; sub(/^[^(]*\(/, "", name); sub(/\).*$/, "", name)
           sub(/^.*\) /, ""); split(FILENAME, path, "/")
           print path[5], $12 + $13, name }' \
        /proc/"$flb_pid"/task/*/stat 2>/dev/null || true
}

check_alive() {
    kill -0 "$flb_pid" 2>/dev/null && return 0
    printf "\nfluent-bit exited; the end of its log:\n" >&2
    tail -20 "$LOG" >&2
    exit 1
}

# ---- setup ------------------------------------------------------------

if [ "$DEMO_ASYNC_ACK" = on ]; then
    acks="asynchronous acknowledgments"
else
    acks="a blocking wait per chunk (async_ack off)"
fi
echo "zerobus_stream demo, run $DEMO_RUN"
echo
echo "Setup"
row "table" "$DEMO_TABLE"
row "offered load" "$(fmt rps "$RATE") records/s, $(fmt mbps "$RATE") MB/s," \
    "from 2 generator inputs"
row "records" "$(fmt count "$RECORD_BYTES") bytes of JSON each"
workers="$DEMO_WORKERS output workers"
[ "$DEMO_WORKERS" = 1 ] && workers="1 output worker"
row "plugin" "$DEMO_STREAMS streams, $workers, $acks"
row "input buffer" "$MEM_BUF_MB MB, which bounds the data in flight"
row "schedule" "${WARMUP} s warmup, then ${DURATION} s measured"
row "log" "$LOG"
echo

"$FLB" -c "$here/zerobus_stream_demo.conf" > "$LOG" 2>&1 &
flb_pid=$!
trap 'kill "$flb_pid" 2>/dev/null || true' EXIT

# The HTTP server starts only once the plugin has opened its streams
printf "Opening %s streams ... " "$DEMO_STREAMS"
started=$(now)
until s=$(sample); do
    check_alive
    if awk -v a="$started" -v b="$(now)" 'BEGIN { exit !(b - a > 120) }'; then
        printf "\nstreams did not open within 120 s\n" >&2
        exit 1
    fi
    sleep 0.2
done
opened=$(sed -nE 's/.*\] ([0-9]+) of [0-9]+ streams open.*/\1/p' "$LOG" | head -1)
took=$(awk -v a="$started" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }')
if [ "$opened" = "$DEMO_STREAMS" ]; then
    echo "all open after $took s"
else
    echo "$opened open after $took s; the rest retry in the background"
fi
printf "Warming up for %s s (not measured) ...\n" "$WARMUP"

sleep "$WARMUP"
check_alive
s=$(sample)
read -r t0 inp0 out0 <<< "$s"
pauses0=$(pauses)
cpu0=$(proc_ticks)
threads0=$(mktemp)
thread_ticks > "$threads0"
t_prev=$t0 out_prev=$out0
end=$(awk -v t="$t0" -v d="$DURATION" 'BEGIN { printf "%.3f", t + d }')

# ---- measuring --------------------------------------------------------

echo
echo "Throughput, counting only records the server acknowledged as durable"
printf "  %7s %10s %13s %16s\n" "time" "MB/s" "records/s" "records so far"
k=1
while :; do
    # Sleep to the next progress mark, the last one being the window's end
    mark=$(awk -v t="$t0" -v k="$k" -v i="$INTERVAL" -v e="$end" \
        'BEGIN { m = t + k * i; printf "%.3f", (m < e ? m : e) }')
    pause=$(awk -v m="$mark" -v n="$(now)" 'BEGIN { printf "%.3f", (m > n ? m - n : 0) }')
    sleep "$pause"
    check_alive
    s=$(sample) || { echo "lost Fluent Bit's metrics endpoint" >&2; exit 1; }
    read -r t inp out <<< "$s"
    dt=$(awk -v a="$t_prev" -v b="$t" 'BEGIN { printf "%.3f", b - a }')
    elapsed=$(awk -v a="$t0" -v b="$t" 'BEGIN { printf "%.0f", b - a }')
    printf "  %5s s %10s %13s %16s\n" "$elapsed" \
        "$(fmt mbps $(( out - out_prev )) "$dt")" \
        "$(fmt rps $(( out - out_prev )) "$dt")" \
        "$(fmt count $(( out - out0 )))"
    t_prev=$t out_prev=$out
    awk -v t="$t" -v e="$end" 'BEGIN { exit !(t >= e) }' && break
    k=$(( k + 1 ))
done

window=$(awk -v a="$t0" -v b="$t" 'BEGIN { printf "%.1f", b - a }')
acked=$(( out - out0 ))
generated=$(( inp - inp0 ))
waits=$(( $(pauses) - pauses0 ))
hz=$(getconf CLK_TCK)
cores=$(awk -v a="$cpu0" -v b="$(proc_ticks)" -v hz="$hz" -v s="$window" \
    'BEGIN { printf "%.2f", (b - a) / hz / s }')
threads1=$(mktemp)
thread_ticks > "$threads1"
# The busiest thread over the window: "<cores> <name>"
read -r busiest busiest_name < <(awk -v hz="$hz" -v s="$window" '
    NR == FNR { start[$1] = $2; next }
    ($1 in start) && $2 - start[$1] > max { max = $2 - start[$1]; name = $3 }
    END { printf "%.2f %s\n", max / hz / s, name }' "$threads0" "$threads1")
rm -f "$threads0" "$threads1"
busiest_pct=$(awk -v b="$busiest" 'BEGIN { printf "%.0f", b * 100 }')
busiest_role=$(thread_role "$busiest_name")

# ---- result -----------------------------------------------------------

echo
echo "Result over $window s"
printf "  %-14s %7s MB/s  %9s records/s   (%s records)\n" "acknowledged" \
    "$(fmt mbps "$acked" "$window")" "$(fmt rps "$acked" "$window")" \
    "$(fmt count "$acked")"
printf "  %-14s %7s MB/s  %9s records/s\n" "generated" \
    "$(fmt mbps "$generated" "$window")" "$(fmt rps "$generated" "$window")"
printf "  %-14s %7s MB/s  %9s records/s\n" "offered" \
    "$(fmt mbps "$RATE")" "$(fmt rps "$RATE")"
row "client CPU" "$cores cores in total"
row "busiest thread" "$busiest_pct% of a core: the $busiest_role"
if awk -v n="$acked" -v s="$window" -v r="$RATE" 'BEGIN { exit !(n / s >= 0.97 * r) }'
then
    row "bottleneck" "none: the plugin kept up with the offered load."
    row "" "Raise DEMO_RATE to find its ceiling."
elif [ "$waits" -eq 0 ]; then
    row "bottleneck" "the load generators: the inputs never had to wait for"
    row "" "the plugin, which had room for more."
elif awk -v b="$busiest" 'BEGIN { exit !(b >= 0.9) }'; then
    row "bottleneck" "Fluent Bit itself: its $busiest_role is saturated."
else
    row "bottleneck" "the network or the service: the inputs paused $waits times"
    row "" "waiting for acknowledgments, while no Fluent Bit thread was"
    row "" "saturated. More streams help on a long round trip."
fi

# ---- stopping ---------------------------------------------------------

echo
printf "Stopping: the inputs stop and the buffered chunks drain ... "
stopped=$(now)
kill -INT "$flb_pid"
wait "$flb_pid" || true
trap - EXIT
echo "done after $(awk -v a="$stopped" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }') s"
# The plugin logs per stream: "... sent N records in M requests, K acknowledged"
total=$(awk '/stream [0-9]+ sent [0-9]+ records .* acknowledged$/ { sum += $(NF - 1) }
             END { printf "%.0f", sum }' "$LOG")
row "acknowledged" "$(fmt count "$total") records in total, warmup and drain included"
problems=$(grep -E "\[(error| warn)\] \[output:zerobus_stream" "$LOG" || true)
if [ -n "$problems" ]; then
    n=$(wc -l <<< "$problems")
    if [ "$n" -gt 5 ]; then
        row "warnings" "the plugin logged $n warnings or errors; the first 5:"
    else
        row "warnings" "the plugin logged $n warnings or errors:"
    fi
    head -5 <<< "$problems" | plugin_lines | sed 's/^/                 /'
fi

# ---- checking the table -----------------------------------------------

echo
if [ -z "${DATABRICKS_STAGING_TOKEN:-}" ]; then
    echo "Checking the table: skipped, DATABRICKS_STAGING_TOKEN is not set"
    exit 0
fi

query="SELECT count(*) FROM $DEMO_TABLE WHERE startswith(device_name, '$DEMO_RUN/')"
echo "Checking the table"
row "query" "$query"
count_rows() {
    python3 -I - "$DEMO_UC_ENDPOINT" "$WAREHOUSE" "$query" <<'EOF'
import json, os, sys, time, urllib.request

workspace, warehouse, query = sys.argv[1:4]
headers = {"Authorization": "Bearer " + os.environ["DATABRICKS_STAGING_TOKEN"],
           "Content-Type": "application/json"}

def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(workspace + path, data=data, method=method,
                                 headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)

st = call("POST", "/api/2.0/sql/statements",
          {"warehouse_id": warehouse, "statement": query, "wait_timeout": "30s"})
# The API often answers PENDING even with a wait timeout: poll until done.
while st["status"]["state"] in ("PENDING", "RUNNING"):
    time.sleep(1)
    st = call("GET", "/api/2.0/sql/statements/" + st["statement_id"])
if st["status"]["state"] != "SUCCEEDED":
    sys.exit("query failed: " + json.dumps(st["status"]))
print(st["result"]["data_array"][0][0])
EOF
}

# Rows become queryable a few seconds after the acknowledgment, so poll the
# count for up to a minute
for _ in $(seq 1 12); do
    rows=$(count_rows)
    [ "$rows" -ge "$total" ] && break
    sleep 5
done
if [ "$rows" -eq "$total" ]; then
    row "result" "OK: all $(fmt count "$total") acknowledged records are in the table"
elif [ "$rows" -gt "$total" ]; then
    row "result" "OK: all $(fmt count "$total") acknowledged records are in the table,"
    row "" "plus $(fmt count $(( rows - total ))) duplicates from retried chunks" \
        "(delivery is at least once)"
else
    row "result" "only $(fmt count "$rows") of $(fmt count "$total") acknowledged" \
        "records are visible after a minute" >&2
    exit 1
fi
