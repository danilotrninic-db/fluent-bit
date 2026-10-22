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

#ifndef FLB_OUT_ZEROBUS_STREAM_H
#define FLB_OUT_ZEROBUS_STREAM_H

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_coro.h>
#include <fluent-bit/flb_scheduler.h>
#include <pthread.h>
#include <zerobus/zerobus.h>

/* Lane states */
#define ZBS_LANE_DOWN     0     /* reopened once no chunk uses it */
#define ZBS_LANE_OPENING  1
#define ZBS_LANE_READY    2

/* One ingest request on a lane, kept until the server acknowledges it */
struct zbs_batch {
    zerobus_offset_t offset;
    size_t bytes;
    struct mk_list _head;
};

/*
 * A lane is one SDK stream with its own connection. Lanes are shared by all
 * output workers: each chunk is sent whole on one lane, and a failure reopens
 * only that lane.
 */
struct zbs_lane {
    int id;
    struct flb_out_zerobus_stream *ctx;
    zerobus_stream_builder_t *builder;  /* used only by the opener thread */
    pthread_t opener;
    int opener_started;

    /* Everything below is protected by mutex */
    pthread_mutex_t mutex;
    pthread_cond_t cond;                /* opener wakeups, open attempts */
    int state;
    int stopping;
    int opened_once;                    /* the first open attempt finished */
    int open_failures;                  /* consecutive, for the backoff */
    uint64_t retry_at_ms;
    int users;                          /* chunks holding the stream */
    zerobus_stream_t *stream;

    /*
     * Ingest admission runs under the mutex, so batches are in offset order
     * and a chunk's last offset covers it and every earlier chunk. Acks are
     * cumulative: offsets at or below 'acked' are durable.
     */
    zerobus_offset_t acked;
    size_t inflight_bytes;
    size_t inflight_batches;
    struct mk_list batches;             /* struct zbs_batch */
    uint64_t total_records;             /* ingested, including retries */
    uint64_t total_batches;
    uint64_t acked_records;             /* in chunks that returned FLB_OK */
};

/* A flush coroutine suspended until its lane acknowledges 'target' */
struct zbs_waiter {
    struct flb_coro *coro;
    struct zbs_lane *lane;
    zerobus_offset_t target;
    uint64_t deadline_ms;
    int result;                         /* FLB_OK or FLB_RETRY, set on resume */
    struct mk_list _head;
};

/*
 * Per output worker: one timer on the worker's event loop polls the lanes
 * and resumes the coroutines whose wait is over. It exists only while
 * coroutines are waiting.
 */
struct zbs_worker {
    struct flb_out_zerobus_stream *ctx;
    struct flb_sched_timer *timer;
    struct mk_list waiters;             /* struct zbs_waiter */
};

struct flb_out_zerobus_stream {
    /* config map */
    flb_sds_t endpoint;
    flb_sds_t unity_catalog_endpoint;
    flb_sds_t table_name;
    flb_sds_t client_id;
    flb_sds_t client_secret;
    flb_sds_t ca_file;
    int streams;
    size_t batch_size;
    size_t buffer_size;
    int flush_timeout;
    int async_ack;
    int ack_poll_interval;

    zerobus_sdk_t *sdk;
    struct zbs_lane *lanes;
    int lane_count;                     /* lanes with mutex and cond set up */
    struct flb_output_instance *ins;
};

#endif
