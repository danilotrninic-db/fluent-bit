/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2026 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Databricks Zerobus output over long-lived gRPC streams, using the pure C
 * SDK. A flush never blocks its worker: it ingests the chunk, then suspends
 * its coroutine until the server acknowledges the chunk's last offset. One
 * timer per worker polls the acknowledged offsets and resumes the chunks
 * whose wait is over, so a worker keeps many chunks in flight. A chunk still
 * returns FLB_OK only once it is durable.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_config_map.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_thread_storage.h>
#include <time.h>

#include "zerobus_stream.h"

/*
 * The SDK rejects a request larger than 10 MiB - 64 KiB, counting 16 bytes of
 * framing per record of a batch.
 */
#define ZBS_MAX_REQUEST_BYTES     (10 * 1024 * 1024 - 64 * 1024)
#define ZBS_RECORD_OVERHEAD       16
#define ZBS_MAX_RECORD_BYTES      (ZBS_MAX_REQUEST_BYTES - ZBS_RECORD_OVERHEAD)

/* The SDK's in-flight limit, in requests. Lanes stay below it, so an ingest
 * call never waits inside the SDK. */
#define ZBS_MAX_INFLIGHT_BATCHES  65536

#define ZBS_MAX_STREAMS           256
#define ZBS_OPEN_BACKOFF_MS       1000
#define ZBS_OPEN_BACKOFF_MAX_MS   30000

/* waiter_check() result while the wait goes on */
#define ZBS_WAITING               -1

/* JSON records back to back in one buffer, with a view of each */
struct zbs_records {
    char *json;
    zerobus_string_view_t *views;
    size_t count;
};

FLB_TLS_DEFINE(struct zbs_worker, zbs_worker_tls);

static uint64_t monotonic_ms(void)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t) now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static const char *status_name(zerobus_status_t status)
{
    switch (status) {
    case ZEROBUS_STATUS_OK:
        return "OK";
    case ZEROBUS_STATUS_INVALID_ARGUMENT:
        return "INVALID_ARGUMENT";
    case ZEROBUS_STATUS_DEADLINE_EXCEEDED:
        return "DEADLINE_EXCEEDED";
    case ZEROBUS_STATUS_CANCELLED:
        return "CANCELLED";
    case ZEROBUS_STATUS_FAILED_PRECONDITION:
        return "FAILED_PRECONDITION";
    case ZEROBUS_STATUS_RESOURCE_EXHAUSTED:
        return "RESOURCE_EXHAUSTED";
    case ZEROBUS_STATUS_UNAUTHENTICATED:
        return "UNAUTHENTICATED";
    case ZEROBUS_STATUS_PERMISSION_DENIED:
        return "PERMISSION_DENIED";
    case ZEROBUS_STATUS_NOT_FOUND:
        return "NOT_FOUND";
    case ZEROBUS_STATUS_UNAVAILABLE:
        return "UNAVAILABLE";
    case ZEROBUS_STATUS_UNIMPLEMENTED:
        return "UNIMPLEMENTED";
    case ZEROBUS_STATUS_OUT_OF_MEMORY:
        return "OUT_OF_MEMORY";
    case ZEROBUS_STATUS_INTERNAL:
        return "INTERNAL";
    default:
        return "UNKNOWN";
    }
}

/* The error's message, valid until the error is freed */
static zerobus_string_view_t error_text(const zerobus_error_t *err)
{
    zerobus_string_view_t msg;

    msg = zerobus_error_message(err);
    if (msg.data == NULL) {
        msg = zerobus_string_view("no details");
    }
    return msg;
}

/* ---- lanes ------------------------------------------------------------ */

/* Drop the batches the server has acknowledged. Mutex held. */
static void lane_release_acked(struct zbs_lane *lane)
{
    struct mk_list *head;
    struct mk_list *tmp;
    struct zbs_batch *batch;

    mk_list_foreach_safe(head, tmp, &lane->batches) {
        batch = mk_list_entry(head, struct zbs_batch, _head);
        if (batch->offset > lane->acked) {
            break;
        }
        lane->inflight_bytes -= batch->bytes;
        lane->inflight_batches--;
        mk_list_del(&batch->_head);
        flb_free(batch);
    }
}

/* Forget every batch, before the stream is replaced. Mutex held. */
static void lane_drop_batches(struct zbs_lane *lane)
{
    struct mk_list *head;
    struct mk_list *tmp;
    struct zbs_batch *batch;

    mk_list_foreach_safe(head, tmp, &lane->batches) {
        batch = mk_list_entry(head, struct zbs_batch, _head);
        mk_list_del(&batch->_head);
        flb_free(batch);
    }
    lane->inflight_bytes = 0;
    lane->inflight_batches = 0;
    lane->acked = -1;
}

/* Take the lane down so its opener reopens it. Mutex held. */
static void lane_fail(struct zbs_lane *lane, zerobus_status_t status,
                      const zerobus_error_t *err)
{
    zerobus_string_view_t msg;

    if (lane->state != ZBS_LANE_READY) {
        return;
    }
    msg = error_text(err);
    flb_plg_warn(lane->ctx->ins, "stream %d failed (%s: %.*s), reopening it",
                 lane->id, status_name(status), (int) msg.len, msg.data);
    lane->state = ZBS_LANE_DOWN;
    lane->retry_at_ms = 0;
    pthread_cond_broadcast(&lane->cond);
}

/* Refresh the acknowledged offset from the SDK, without waiting. Mutex held. */
static void lane_poll(struct zbs_lane *lane)
{
    zerobus_offset_t acked = -1;
    zerobus_error_t *err = NULL;
    zerobus_status_t status;

    if (lane->state != ZBS_LANE_READY) {
        return;
    }
    status = zerobus_stream_get_acked_offset(lane->stream, &acked, &err);
    if (acked > lane->acked) {
        lane->acked = acked;
        lane_release_acked(lane);
    }
    if (status != ZEROBUS_STATUS_OK) {
        lane_fail(lane, status, err);
    }
    zerobus_error_free(err);
}

/*
 * Each lane has an opener thread, because opening a stream blocks: it
 * connects, mints an OAuth token and waits for the server. The opener (re)opens
 * the lane whenever it is down and no chunk holds the old stream any longer,
 * backing off after failures.
 */
static void *lane_opener(void *data)
{
    uint64_t delay;
    struct timespec until;
    zerobus_status_t status;
    zerobus_error_t *err;
    zerobus_stream_t *old;
    zerobus_stream_t *stream;
    zerobus_string_view_t msg;
    struct zbs_lane *lane = data;
    struct flb_output_instance *ins = lane->ctx->ins;

    pthread_mutex_lock(&lane->mutex);
    while (!lane->stopping) {
        if (lane->state != ZBS_LANE_DOWN || lane->users > 0) {
            pthread_cond_wait(&lane->cond, &lane->mutex);
            continue;
        }
        if (monotonic_ms() < lane->retry_at_ms) {
            until.tv_sec = lane->retry_at_ms / 1000;
            until.tv_nsec = (lane->retry_at_ms % 1000) * 1000000;
            pthread_cond_timedwait(&lane->cond, &lane->mutex, &until);
            continue;
        }

        old = lane->stream;
        lane->stream = NULL;
        lane->state = ZBS_LANE_OPENING;
        lane_drop_batches(lane);
        pthread_mutex_unlock(&lane->mutex);

        /* No chunk uses the old stream: free it without flushing, its chunks
         * were retried */
        zerobus_stream_free(old);
        err = NULL;
        stream = NULL;
        status = zerobus_stream_builder_build(lane->builder, &stream, &err);

        pthread_mutex_lock(&lane->mutex);
        if (status == ZEROBUS_STATUS_OK) {
            lane->stream = stream;
            lane->state = ZBS_LANE_READY;
            lane->open_failures = 0;
            flb_plg_info(ins, "stream %d open", lane->id);
        }
        else {
            delay = (uint64_t) ZBS_OPEN_BACKOFF_MS <<
                    (lane->open_failures < 5 ? lane->open_failures : 5);
            if (delay > ZBS_OPEN_BACKOFF_MAX_MS) {
                delay = ZBS_OPEN_BACKOFF_MAX_MS;
            }
            lane->state = ZBS_LANE_DOWN;
            lane->open_failures++;
            lane->retry_at_ms = monotonic_ms() + delay;
            msg = error_text(err);
            flb_plg_error(ins, "stream %d could not be opened (%s: %.*s)",
                          lane->id, status_name(status), (int) msg.len,
                          msg.data);
        }
        lane->opened_once = FLB_TRUE;
        pthread_cond_broadcast(&lane->cond);
        zerobus_error_free(err);
    }

    /* Stopping: no chunk is left. Closing here closes every lane at once,
     * each taking about a round trip, instead of one after another. */
    stream = lane->state == ZBS_LANE_READY ? lane->stream : NULL;
    pthread_mutex_unlock(&lane->mutex);
    if (stream != NULL) {
        err = NULL;
        status = zerobus_stream_close(stream, &err);
        if (status != ZEROBUS_STATUS_OK) {
            msg = error_text(err);
            flb_plg_warn(ins, "stream %d did not close cleanly (%s: %.*s)",
                         lane->id, status_name(status), (int) msg.len,
                         msg.data);
        }
        zerobus_error_free(err);
    }
    return NULL;
}

/*
 * Pick the open lane with the fewest unacknowledged bytes, then the fewest
 * chunks, and hold it until lane_release(). NULL when no lane is open. Ties go
 * to the lowest lane, so light traffic keeps reusing a few connections: a TCP
 * connection idle for long restarts from a small congestion window.
 */
static struct zbs_lane *lane_acquire(struct flb_out_zerobus_stream *ctx)
{
    int i;
    struct zbs_lane *lane;
    struct zbs_lane *best = NULL;
    size_t best_bytes = 0;
    int best_users = 0;

    for (i = 0; i < ctx->streams; i++) {
        lane = &ctx->lanes[i];
        pthread_mutex_lock(&lane->mutex);
        if (lane->state == ZBS_LANE_READY &&
            (best == NULL || lane->inflight_bytes < best_bytes ||
             (lane->inflight_bytes == best_bytes &&
              lane->users < best_users))) {
            best = lane;
            best_bytes = lane->inflight_bytes;
            best_users = lane->users;
        }
        pthread_mutex_unlock(&lane->mutex);
    }
    if (best == NULL) {
        return NULL;
    }

    lane = best;
    pthread_mutex_lock(&lane->mutex);
    if (lane->state == ZBS_LANE_READY) {
        lane->users++;
    }
    else {
        /* failed meanwhile: let Fluent Bit retry the chunk */
        best = NULL;
    }
    pthread_mutex_unlock(&lane->mutex);
    return best;
}

static void lane_release(struct zbs_lane *lane)
{
    pthread_mutex_lock(&lane->mutex);
    lane->users--;
    if (lane->users == 0 && lane->state == ZBS_LANE_DOWN) {
        pthread_cond_broadcast(&lane->cond);
    }
    pthread_mutex_unlock(&lane->mutex);
}

/* ---- waiting for acknowledgments ---------------------------------------- */

/* FLB_OK once the lane acknowledged the waiter's target, FLB_RETRY if the
 * lane failed or the chunk timed out, else ZBS_WAITING. */
static int waiter_check(struct zbs_waiter *waiter, uint64_t now)
{
    int ret = ZBS_WAITING;
    int timed_out = FLB_FALSE;
    zerobus_offset_t acked;
    struct zbs_lane *lane = waiter->lane;

    pthread_mutex_lock(&lane->mutex);
    if (lane->acked < waiter->target) {
        lane_poll(lane);
    }
    acked = lane->acked;
    if (acked >= waiter->target) {
        ret = FLB_OK;
    }
    else if (lane->state != ZBS_LANE_READY) {
        ret = FLB_RETRY;
    }
    else if (now >= waiter->deadline_ms) {
        ret = FLB_RETRY;
        timed_out = FLB_TRUE;
    }
    pthread_mutex_unlock(&lane->mutex);

    if (timed_out) {
        flb_plg_warn(lane->ctx->ins,
                     "stream %d did not acknowledge offset %lld within "
                     "flush_timeout (acknowledged %lld), retrying the chunk",
                     lane->id, (long long) waiter->target, (long long) acked);
    }
    return ret;
}

/*
 * The worker's poll timer: resume every coroutine whose wait is over. A
 * single cumulative acknowledgment can complete many chunks at once.
 */
static void cb_poll_timer(struct flb_config *config, void *data)
{
    int ret;
    uint64_t now;
    struct mk_list ready;
    struct mk_list *head;
    struct mk_list *tmp;
    struct zbs_waiter *waiter;
    struct zbs_worker *worker = data;

    (void) config;

    mk_list_init(&ready);
    now = monotonic_ms();
    mk_list_foreach_safe(head, tmp, &worker->waiters) {
        waiter = mk_list_entry(head, struct zbs_waiter, _head);
        ret = waiter_check(waiter, now);
        if (ret == ZBS_WAITING) {
            continue;
        }
        waiter->result = ret;
        mk_list_del(&waiter->_head);
        mk_list_add(&waiter->_head, &ready);
    }

    /* A resumed coroutine may wait again, adding itself to worker->waiters */
    while (mk_list_is_empty(&ready) != 0) {
        waiter = mk_list_entry_first(&ready, struct zbs_waiter, _head);
        mk_list_del(&waiter->_head);
        flb_coro_resume(waiter->coro);
    }

    if (mk_list_is_empty(&worker->waiters) == 0) {
        /* Nothing waits: the scheduler frees the timer after this callback */
        flb_sched_timer_invalidate(worker->timer);
        worker->timer = NULL;
    }
}

/*
 * Suspend the flush coroutine until the lane acknowledges target, the lane
 * fails, or the chunk's flush_timeout passes. The worker runs other chunks
 * meanwhile.
 */
static int wait_acked(struct zbs_worker *worker, struct zbs_waiter *waiter,
                      struct zbs_lane *lane, zerobus_offset_t target)
{
    int ret;

    waiter->lane = lane;
    waiter->target = target;
    ret = waiter_check(waiter, monotonic_ms());
    if (ret != ZBS_WAITING) {
        return ret;
    }

    if (worker->timer == NULL) {
        ret = flb_sched_timer_cb_create(flb_sched_ctx_get(),
                                        FLB_SCHED_TIMER_CB_PERM,
                                        worker->ctx->ack_poll_interval,
                                        cb_poll_timer, worker, &worker->timer);
        if (ret != 0) {
            flb_plg_error(worker->ctx->ins, "cannot create the poll timer");
            worker->timer = NULL;
            return FLB_RETRY;
        }
    }
    waiter->result = ZBS_WAITING;
    mk_list_add(&waiter->_head, &worker->waiters);
    flb_coro_yield(waiter->coro, FLB_FALSE);

    /* cb_poll_timer() removed the waiter from the list and set the result */
    return waiter->result;
}

/* async_ack off: block the worker until the whole lane is acknowledged. This
 * is the per-chunk blocking wait the design avoids, kept for comparison. */
static int flush_blocking(struct zbs_lane *lane)
{
    zerobus_status_t status;
    zerobus_stream_t *stream;
    zerobus_error_t *err = NULL;
    zerobus_string_view_t msg;

    pthread_mutex_lock(&lane->mutex);
    stream = lane->stream;
    pthread_mutex_unlock(&lane->mutex);

    status = zerobus_stream_flush(stream, &err);
    if (status == ZEROBUS_STATUS_OK) {
        return FLB_OK;
    }
    if (status == ZEROBUS_STATUS_DEADLINE_EXCEEDED) {
        msg = error_text(err);
        flb_plg_warn(lane->ctx->ins, "stream %d: %.*s, retrying the chunk",
                     lane->id, (int) msg.len, msg.data);
    }
    else {
        pthread_mutex_lock(&lane->mutex);
        lane_fail(lane, status, err);
        pthread_mutex_unlock(&lane->mutex);
    }
    zerobus_error_free(err);
    return FLB_RETRY;
}

/* ---- ingest ----------------------------------------------------------- */

static void records_destroy(struct zbs_records *records)
{
    flb_free(records->json);
    flb_free(records->views);
    memset(records, 0, sizeof(*records));
}

/*
 * Encode each log body as one JSON record, back to back in one buffer, with
 * no allocation per record. Growing the buffer may move it, so the views are
 * filled in at the end. A record that cannot be encoded within the SDK's
 * request limit is dropped.
 */
static int records_encode(struct flb_out_zerobus_stream *ctx,
                          struct flb_event_chunk *chunk, int escape_unicode,
                          struct zbs_records *records)
{
    int ret;
    int len;
    char *json;
    size_t i;
    size_t used = 0;
    size_t size;
    size_t avail;
    size_t dropped = 0;
    size_t views_size;
    zerobus_string_view_t *views;
    struct flb_log_event event;
    struct flb_log_event_decoder decoder;

    memset(records, 0, sizeof(*records));

    ret = flb_log_event_decoder_init(&decoder, (char *) chunk->data,
                                     chunk->size);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins, "cannot decode the chunk (%d)", ret);
        return FLB_ERROR;
    }

    /* JSON usually takes a little more room than MessagePack */
    size = chunk->size + chunk->size / 2 + 256;
    views_size = chunk->total_events > 0 ? chunk->total_events : 64;
    records->json = flb_malloc(size);
    records->views = flb_malloc(views_size * sizeof(zerobus_string_view_t));
    if (records->json == NULL || records->views == NULL) {
        flb_errno();
        goto oom;
    }

    while (flb_log_event_decoder_next(&decoder, &event) ==
           FLB_EVENT_DECODER_SUCCESS) {
        while (1) {
            avail = size - used;
            if (avail > ZBS_MAX_RECORD_BYTES + 1) {
                avail = ZBS_MAX_RECORD_BYTES + 1;
            }
            /* Writes len bytes and a NUL, which the next record overwrites */
            len = flb_msgpack_to_json(records->json + used, avail, event.body,
                                      escape_unicode);
            if (len > 0 || avail == ZBS_MAX_RECORD_BYTES + 1) {
                break;
            }
            /* Too small: grow the buffer and encode the record again */
            size *= 2;
            json = flb_realloc(records->json, size);
            if (json == NULL) {
                flb_errno();
                goto oom;
            }
            records->json = json;
        }
        if (len <= 0) {
            dropped++;
            continue;
        }
        if (records->count == views_size) {
            views_size *= 2;
            views = flb_realloc(records->views,
                                views_size * sizeof(zerobus_string_view_t));
            if (views == NULL) {
                flb_errno();
                goto oom;
            }
            records->views = views;
        }
        records->views[records->count++].len = len;
        used += len;
    }

    ret = flb_log_event_decoder_get_last_result(&decoder);
    flb_log_event_decoder_destroy(&decoder);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins, "malformed chunk (decoder error %d)", ret);
        records_destroy(records);
        return FLB_ERROR;
    }

    used = 0;
    for (i = 0; i < records->count; i++) {
        records->views[i].data = records->json + used;
        used += records->views[i].len;
    }
    if (dropped > 0) {
        flb_plg_error(ctx->ins, "dropped %zu records that do not fit in a "
                      "%d-byte request", dropped, ZBS_MAX_RECORD_BYTES);
    }
    return FLB_OK;

oom:
    flb_log_event_decoder_destroy(&decoder);
    records_destroy(records);
    return FLB_RETRY;
}

/*
 * Ingest the records on the lane as requests of at most batch_size bytes.
 * Before each request, wait while the lane already holds buffer_size
 * unacknowledged bytes. *target receives the last request's offset: once it
 * is acknowledged, the whole chunk is durable.
 */
static int lane_admit(struct flb_out_zerobus_stream *ctx,
                      struct zbs_worker *worker, struct zbs_waiter *waiter,
                      struct zbs_lane *lane, struct zbs_records *records,
                      zerobus_offset_t *target)
{
    int ret;
    size_t i = 0;
    size_t end;
    size_t bytes;
    zerobus_offset_t offset;
    zerobus_offset_t oldest;
    zerobus_status_t status;
    zerobus_error_t *err;
    zerobus_string_view_t msg;
    struct zbs_batch *batch;

    while (i < records->count) {
        bytes = 0;
        end = i;
        while (end < records->count &&
               (end == i || bytes + records->views[end].len +
                ZBS_RECORD_OVERHEAD <= ctx->batch_size)) {
            bytes += records->views[end].len + ZBS_RECORD_OVERHEAD;
            end++;
        }

        batch = flb_malloc(sizeof(struct zbs_batch));
        if (batch == NULL) {
            flb_errno();
            return FLB_RETRY;
        }

        pthread_mutex_lock(&lane->mutex);
        while (1) {
            lane_poll(lane);
            if (lane->state != ZBS_LANE_READY) {
                pthread_mutex_unlock(&lane->mutex);
                flb_free(batch);
                return FLB_RETRY;
            }
            if (lane->inflight_batches == 0 ||
                (lane->inflight_bytes + bytes <= ctx->buffer_size &&
                 lane->inflight_batches < ZBS_MAX_INFLIGHT_BATCHES)) {
                break;
            }
            /* The lane is full: wait for its oldest request */
            oldest = mk_list_entry_first(&lane->batches, struct zbs_batch,
                                         _head)->offset;
            pthread_mutex_unlock(&lane->mutex);
            ret = wait_acked(worker, waiter, lane, oldest);
            if (ret != FLB_OK) {
                flb_free(batch);
                return ret;
            }
            pthread_mutex_lock(&lane->mutex);
        }

        /* The SDK copies the records: the chunk's buffer can go after this */
        err = NULL;
        status = zerobus_stream_ingest_json_records(lane->stream,
                                                    records->views + i,
                                                    end - i, &offset, &err);
        if (status == ZEROBUS_STATUS_OK) {
            batch->offset = offset;
            batch->bytes = bytes;
            mk_list_add(&batch->_head, &lane->batches);
            lane->inflight_bytes += bytes;
            lane->inflight_batches++;
            lane->total_records += end - i;
            lane->total_batches++;
            *target = offset;
            batch = NULL;
        }
        else if (status != ZEROBUS_STATUS_INVALID_ARGUMENT) {
            lane_fail(lane, status, err);
        }
        pthread_mutex_unlock(&lane->mutex);

        if (status != ZEROBUS_STATUS_OK) {
            flb_free(batch);
            ret = FLB_RETRY;
            if (status == ZEROBUS_STATUS_INVALID_ARGUMENT) {
                msg = error_text(err);
                flb_plg_error(ctx->ins, "records rejected (%.*s), dropping "
                              "the chunk", (int) msg.len, msg.data);
                ret = FLB_ERROR;
            }
            zerobus_error_free(err);
            return ret;
        }
        i = end;
    }
    return FLB_OK;
}

static void cb_zerobus_stream_flush(struct flb_event_chunk *event_chunk,
                                    struct flb_output_flush *out_flush,
                                    struct flb_input_instance *i_ins,
                                    void *out_context,
                                    struct flb_config *config)
{
    int ret;
    size_t count;
    zerobus_offset_t target = -1;
    struct zbs_lane *lane;
    struct zbs_worker *worker;
    struct zbs_waiter waiter;
    struct zbs_records records;
    struct flb_out_zerobus_stream *ctx = out_context;

    (void) out_flush;
    (void) i_ins;

    worker = FLB_TLS_GET(zbs_worker_tls);
    if (worker == NULL) {
        flb_plg_error(ctx->ins, "flush outside an output worker");
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }

    ret = records_encode(ctx, event_chunk, config->json_escape_unicode,
                         &records);
    if (ret != FLB_OK || records.count == 0) {
        records_destroy(&records);
        FLB_OUTPUT_RETURN(ret);
    }
    count = records.count;

    lane = lane_acquire(ctx);
    if (lane == NULL) {
        /* The engine logs the retry; lane failures were logged already */
        records_destroy(&records);
        flb_plg_debug(ctx->ins, "no stream is open, retrying the chunk");
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }

    memset(&waiter, 0, sizeof(waiter));
    waiter.coro = flb_coro_get();
    waiter.deadline_ms = monotonic_ms() + (uint64_t) ctx->flush_timeout * 1000;

    ret = lane_admit(ctx, worker, &waiter, lane, &records, &target);
    /* The SDK holds copies now; Fluent Bit keeps the chunk for a retry */
    records_destroy(&records);
    if (ret == FLB_OK) {
        if (ctx->async_ack) {
            ret = wait_acked(worker, &waiter, lane, target);
        }
        else {
            ret = flush_blocking(lane);
        }
    }
    if (ret == FLB_OK) {
        pthread_mutex_lock(&lane->mutex);
        lane->acked_records += count;
        pthread_mutex_unlock(&lane->mutex);
        flb_plg_debug(ctx->ins, "stream %d acknowledged %zu records up to "
                      "offset %lld", lane->id, count, (long long) target);
    }
    lane_release(lane);
    FLB_OUTPUT_RETURN(ret);
}

/* ---- setup and teardown ----------------------------------------------- */

static int check_config(struct flb_out_zerobus_stream *ctx)
{
    struct flb_output_instance *ins = ctx->ins;

    if (!ctx->endpoint || !ctx->unity_catalog_endpoint || !ctx->table_name ||
        !ctx->client_id || !ctx->client_secret ||
        flb_sds_len(ctx->endpoint) == 0 ||
        flb_sds_len(ctx->unity_catalog_endpoint) == 0 ||
        flb_sds_len(ctx->table_name) == 0 ||
        flb_sds_len(ctx->client_id) == 0 ||
        flb_sds_len(ctx->client_secret) == 0) {
        flb_plg_error(ins, "endpoint, unity_catalog_endpoint, table_name, "
                      "client_id and client_secret are required");
        return -1;
    }
    if (ctx->streams < 1 || ctx->streams > ZBS_MAX_STREAMS) {
        flb_plg_error(ins, "streams must be between 1 and %d",
                      ZBS_MAX_STREAMS);
        return -1;
    }
    if (ctx->batch_size < 1 || ctx->batch_size > ZBS_MAX_REQUEST_BYTES) {
        flb_plg_error(ins, "batch_size must be between 1 and %d bytes",
                      ZBS_MAX_REQUEST_BYTES);
        return -1;
    }
    if (ctx->buffer_size < ctx->batch_size) {
        flb_plg_error(ins, "buffer_size must be at least batch_size");
        return -1;
    }
    if (ctx->flush_timeout < 1) {
        flb_plg_error(ins, "flush_timeout must be at least 1s");
        return -1;
    }
    if (ctx->ack_poll_interval < 1 || ctx->ack_poll_interval > 1000) {
        flb_plg_error(ins, "ack_poll_interval must be between 1 and 1000 ms");
        return -1;
    }
    if (ins->tp_workers < 1) {
        flb_plg_error(ins, "workers must be at least 1: acknowledgments are "
                      "polled on the worker's event loop");
        return -1;
    }
    return 0;
}

static int sdk_create(struct flb_out_zerobus_stream *ctx)
{
    zerobus_status_t status;
    zerobus_error_t *err = NULL;
    zerobus_sdk_builder_t *builder = NULL;
    zerobus_string_view_t msg;

    status = zerobus_sdk_builder_new(&builder, &err);
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_sdk_builder_set_endpoint(
            builder, zerobus_string_view(ctx->endpoint), &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_sdk_builder_set_unity_catalog_endpoint(
            builder, zerobus_string_view(ctx->unity_catalog_endpoint), &err);
    }
    if (status == ZEROBUS_STATUS_OK && ctx->ca_file) {
        status = zerobus_sdk_builder_set_tls_ca_file(
            builder, zerobus_string_view(ctx->ca_file), &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_sdk_builder_build(builder, &ctx->sdk, &err);
    }
    zerobus_sdk_builder_free(builder);

    if (status != ZEROBUS_STATUS_OK) {
        msg = error_text(err);
        flb_plg_error(ctx->ins, "invalid SDK configuration (%s: %.*s)",
                      status_name(status), (int) msg.len, msg.data);
        zerobus_error_free(err);
        return -1;
    }
    return 0;
}

/* Each lane gets its own builder: openers build concurrently, and a builder
 * requires exclusive access. */
static int lane_builder_create(struct flb_out_zerobus_stream *ctx,
                               zerobus_stream_builder_t **out_builder)
{
    zerobus_status_t status;
    zerobus_error_t *err = NULL;
    zerobus_stream_builder_t *builder = NULL;
    zerobus_string_view_t msg;

    status = zerobus_stream_builder_new(ctx->sdk, &builder, &err);
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_stream_builder_set_table(
            builder, zerobus_string_view(ctx->table_name), &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_stream_builder_set_oauth(
            builder, zerobus_string_view(ctx->client_id),
            zerobus_string_view(ctx->client_secret), &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_stream_builder_set_max_inflight_records(
            builder, ZBS_MAX_INFLIGHT_BATCHES, &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        /* The SDK reconnects and resends unacknowledged requests itself; a
         * lane fails only once that gives up */
        status = zerobus_stream_builder_set_recovery(builder, true, &err);
    }
    if (status == ZEROBUS_STATUS_OK) {
        status = zerobus_stream_builder_set_flush_timeout_ms(
            builder, (uint64_t) ctx->flush_timeout * 1000, &err);
    }

    if (status != ZEROBUS_STATUS_OK) {
        msg = error_text(err);
        flb_plg_error(ctx->ins, "invalid stream configuration (%s: %.*s)",
                      status_name(status), (int) msg.len, msg.data);
        zerobus_error_free(err);
        zerobus_stream_builder_free(builder);
        return -1;
    }
    *out_builder = builder;
    return 0;
}

static int lane_init(struct flb_out_zerobus_stream *ctx, struct zbs_lane *lane,
                     int id)
{
    pthread_condattr_t attr;

    lane->id = id;
    lane->ctx = ctx;
    lane->state = ZBS_LANE_DOWN;
    lane->acked = -1;
    mk_list_init(&lane->batches);

    if (pthread_mutex_init(&lane->mutex, NULL) != 0) {
        return -1;
    }
    /* The opener's backoff deadlines are monotonic */
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (pthread_cond_init(&lane->cond, &attr) != 0) {
        pthread_condattr_destroy(&attr);
        pthread_mutex_destroy(&lane->mutex);
        return -1;
    }
    pthread_condattr_destroy(&attr);
    return 0;
}

static void ctx_destroy(struct flb_out_zerobus_stream *ctx)
{
    int i;
    struct zbs_lane *lane;

    for (i = 0; i < ctx->lane_count; i++) {
        lane = &ctx->lanes[i];
        pthread_mutex_lock(&lane->mutex);
        lane->stopping = FLB_TRUE;
        pthread_cond_broadcast(&lane->cond);
        pthread_mutex_unlock(&lane->mutex);
    }
    /* Each opener closes its lane's stream on the way out */
    for (i = 0; i < ctx->lane_count; i++) {
        lane = &ctx->lanes[i];
        if (lane->opener_started) {
            pthread_join(lane->opener, NULL);
        }
    }

    for (i = 0; i < ctx->lane_count; i++) {
        lane = &ctx->lanes[i];
        zerobus_stream_free(lane->stream);
        if (lane->total_batches > 0) {
            flb_plg_info(ctx->ins, "stream %d sent %llu records in %llu "
                         "requests, %llu acknowledged", lane->id,
                         (unsigned long long) lane->total_records,
                         (unsigned long long) lane->total_batches,
                         (unsigned long long) lane->acked_records);
        }
        lane_drop_batches(lane);
        zerobus_stream_builder_free(lane->builder);
        pthread_cond_destroy(&lane->cond);
        pthread_mutex_destroy(&lane->mutex);
    }
    flb_free(ctx->lanes);
    zerobus_sdk_free(ctx->sdk);
    flb_free(ctx);
}

static int cb_zerobus_stream_init(struct flb_output_instance *ins,
                                  struct flb_config *config, void *data)
{
    int i;
    int ready = 0;
    struct zbs_lane *lane;
    struct flb_out_zerobus_stream *ctx;

    (void) config;
    (void) data;

    ctx = flb_calloc(1, sizeof(struct flb_out_zerobus_stream));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;

    if (flb_output_config_map_set(ins, (void *) ctx) == -1 ||
        check_config(ctx) != 0) {
        flb_free(ctx);
        return -1;
    }

    FLB_TLS_INIT(zbs_worker_tls);

    if (sdk_create(ctx) != 0) {
        ctx_destroy(ctx);
        return -1;
    }

    ctx->lanes = flb_calloc(ctx->streams, sizeof(struct zbs_lane));
    if (!ctx->lanes) {
        flb_errno();
        ctx_destroy(ctx);
        return -1;
    }
    for (i = 0; i < ctx->streams; i++) {
        lane = &ctx->lanes[i];
        if (lane_init(ctx, lane, i) != 0) {
            flb_plg_error(ins, "cannot initialize stream %d", i);
            ctx_destroy(ctx);
            return -1;
        }
        ctx->lane_count++;
        if (lane_builder_create(ctx, &lane->builder) != 0) {
            ctx_destroy(ctx);
            return -1;
        }
    }

    /* Open every lane concurrently, then wait for the first attempts */
    for (i = 0; i < ctx->streams; i++) {
        lane = &ctx->lanes[i];
        if (pthread_create(&lane->opener, NULL, lane_opener, lane) != 0) {
            flb_plg_error(ins, "cannot start the opener of stream %d", i);
            ctx_destroy(ctx);
            return -1;
        }
        lane->opener_started = FLB_TRUE;
    }
    for (i = 0; i < ctx->streams; i++) {
        lane = &ctx->lanes[i];
        pthread_mutex_lock(&lane->mutex);
        while (!lane->opened_once) {
            pthread_cond_wait(&lane->cond, &lane->mutex);
        }
        if (lane->state == ZBS_LANE_READY) {
            ready++;
        }
        pthread_mutex_unlock(&lane->mutex);
    }
    if (ready == 0) {
        flb_plg_error(ins, "no stream to %s could be opened", ctx->table_name);
        ctx_destroy(ctx);
        return -1;
    }

    flb_plg_info(ins, "%d of %d streams open to %s (async_ack=%s, "
                 "batch_size=%zu, buffer_size=%zu)", ready, ctx->streams,
                 ctx->table_name, ctx->async_ack ? "on" : "off",
                 ctx->batch_size, ctx->buffer_size);
    flb_output_set_context(ins, ctx);
    return 0;
}

static int cb_zerobus_stream_worker_init(void *data, struct flb_config *config)
{
    struct zbs_worker *worker;

    (void) config;

    worker = flb_calloc(1, sizeof(struct zbs_worker));
    if (!worker) {
        flb_errno();
        return -1;
    }
    worker->ctx = data;
    mk_list_init(&worker->waiters);
    FLB_TLS_SET(zbs_worker_tls, worker);
    return 0;
}

/* Runs once the worker has no flush left, so nothing waits any more */
static int cb_zerobus_stream_worker_exit(void *data, struct flb_config *config)
{
    struct zbs_worker *worker;

    (void) data;
    (void) config;

    worker = FLB_TLS_GET(zbs_worker_tls);
    if (worker == NULL) {
        return 0;
    }
    if (worker->timer != NULL) {
        flb_sched_timer_cb_destroy(worker->timer);
    }
    flb_free(worker);
    FLB_TLS_SET(zbs_worker_tls, NULL);
    return 0;
}

static int cb_zerobus_stream_exit(void *data, struct flb_config *config)
{
    struct flb_out_zerobus_stream *ctx = data;

    (void) config;

    if (ctx) {
        ctx_destroy(ctx);
    }
    return 0;
}

static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "endpoint", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, endpoint),
     "Zerobus ingest endpoint, such as "
     "https://<workspace-id>.zerobus.<region>.cloud.databricks.com"
    },
    {
     FLB_CONFIG_MAP_STR, "unity_catalog_endpoint", NULL,
     0, FLB_TRUE,
     offsetof(struct flb_out_zerobus_stream, unity_catalog_endpoint),
     "Workspace URL that mints the OAuth tokens, such as "
     "https://<workspace>.cloud.databricks.com"
    },
    {
     FLB_CONFIG_MAP_STR, "table_name", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, table_name),
     "Target table, catalog.schema.table"
    },
    {
     FLB_CONFIG_MAP_STR, "client_id", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, client_id),
     "Service principal application id"
    },
    {
     FLB_CONFIG_MAP_STR, "client_secret", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, client_secret),
     "Service principal OAuth secret, best passed as ${VAR}"
    },
    {
     FLB_CONFIG_MAP_STR, "ca_file", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, ca_file),
     "PEM file of CA certificates to trust instead of the system roots"
    },
    {
     FLB_CONFIG_MAP_INT, "streams", "1",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, streams),
     "Streams, each on its own connection, shared by all workers"
    },
    {
     FLB_CONFIG_MAP_SIZE, "batch_size", "4M",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, batch_size),
     "Largest ingest request; a chunk is split into requests of this size"
    },
    {
     FLB_CONFIG_MAP_SIZE, "buffer_size", "64M",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, buffer_size),
     "Unacknowledged bytes allowed per stream before new chunks wait"
    },
    {
     FLB_CONFIG_MAP_TIME, "flush_timeout", "30s",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, flush_timeout),
     "How long a chunk may wait to be sent and acknowledged before it is "
     "retried"
    },
    {
     FLB_CONFIG_MAP_BOOL, "async_ack", "true",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, async_ack),
     "Wait for acknowledgments without blocking the worker; off blocks "
     "each flush, for comparison"
    },
    {
     FLB_CONFIG_MAP_INT, "ack_poll_interval", "2",
     0, FLB_TRUE, offsetof(struct flb_out_zerobus_stream, ack_poll_interval),
     "Milliseconds between acknowledgment polls while chunks wait"
    },

    /* EOF */
    {0}
};

struct flb_output_plugin out_zerobus_stream_plugin = {
    .name           = "zerobus_stream",
    .description    = "Databricks Zerobus streaming ingest (pure C SDK)",
    .cb_init        = cb_zerobus_stream_init,
    .cb_flush       = cb_zerobus_stream_flush,
    .cb_exit        = cb_zerobus_stream_exit,
    .cb_worker_init = cb_zerobus_stream_worker_init,
    .cb_worker_exit = cb_zerobus_stream_worker_exit,
    .config_map     = config_map,
    .event_type     = FLB_OUTPUT_LOGS,
    .workers        = 1,
};
