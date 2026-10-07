/*
 * bench - closed-loop random 4 KiB I/O against the emulated controller.
 * usage: bench [threads] [queues] [depth] [seconds] [read_pct] [lat_us]
 * Latency is submit() -> completion callback, measured per command.
 */
#define _GNU_SOURCE
#include "qp_driver.h"

#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define HIST_US 20000          /* 1 us buckets up to 20 ms, last bucket = overflow */
static uint64_t hist[HIST_US + 1];   /* written only by the reaper thread (callbacks) */

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

typedef struct { sem_t *sem; uint64_t t0; atomic_int *errs; } req_t;

static void done(void *ctx, int st, uint32_t r)
{
    (void)r;
    req_t *q = ctx;
    uint64_t us = (now_ns() - q->t0) / 1000;
    hist[us > HIST_US ? HIST_US : us]++;
    if (st) atomic_fetch_add(q->errs, 1);
    sem_post(q->sem);
}

typedef struct {
    qp_driver *drv; unsigned q, window; double seconds; unsigned read_pct;
    uint64_t base_lba; uint64_t ops; uint64_t rng; atomic_int *errs;
} bt_t;

static uint64_t xs(uint64_t *s) { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s; }

static void *worker(void *arg)
{
    bt_t *t = arg;
    sem_t sem; sem_init(&sem, 0, t->window);
    qp_dma *buf = calloc(t->window, sizeof *buf);
    req_t *req = calloc(t->window, sizeof *req);
    for (unsigned i = 0; i < t->window; i++) {
        if (qp_dma_alloc(t->drv, 4096, &buf[i])) { fprintf(stderr, "dma alloc failed\n"); exit(1); }
        memset(buf[i].va, (int)i, 4096);
    }
    uint64_t end = now_ns() + (uint64_t)(t->seconds * 1e9);
    unsigned slot = 0;
    while (now_ns() < end) {
        sem_wait(&sem);                                   /* at most `window` outstanding */
        uint64_t r = xs(&t->rng);
        qp_cmd c = { .opc = (r % 100) < t->read_pct ? QP_OP_READ : QP_OP_WRITE,
                     .lba = t->base_lba + ((r >> 8) % 8192) * 8, .nblk = 8, .iova = buf[slot].iova };
        req[slot] = (req_t){ &sem, now_ns(), t->errs };
        if (qp_submit(t->drv, t->q, &c, done, &req[slot])) { atomic_fetch_add(t->errs, 1); sem_post(&sem); break; }
        slot = (slot + 1) % t->window;
        t->ops++;
    }
    for (unsigned i = 0; i < t->window; i++) sem_wait(&sem);    /* drain */
    for (unsigned i = 0; i < t->window; i++) qp_dma_free(t->drv, &buf[i]);
    free(buf); free(req); sem_destroy(&sem);
    return NULL;
}

static double pct(uint64_t total, double p)
{
    uint64_t want = (uint64_t)((double)total * p), seen = 0;
    for (unsigned i = 0; i <= HIST_US; i++) { seen += hist[i]; if (seen >= want) return (double)i; }
    return HIST_US;
}

int main(int argc, char **argv)
{
    unsigned threads = argc > 1 ? (unsigned)atoi(argv[1]) : 4;
    unsigned queues  = argc > 2 ? (unsigned)atoi(argv[2]) : 4;
    unsigned depth   = argc > 3 ? (unsigned)atoi(argv[3]) : 128;
    double   secs    = argc > 4 ? atof(argv[4]) : 3.0;
    unsigned rd      = argc > 5 ? (unsigned)atoi(argv[5]) : 70;
    unsigned lat     = argc > 6 ? (unsigned)atoi(argv[6]) : 20;

    qp_dev_config dc = { .max_queues = queues, .capacity_blocks = 1u << 18, .arena_bytes = 64u << 20,
                         .lat_min_us = lat / 2, .lat_max_us = lat + lat / 2 };
    qp_device *dev = qp_dev_create(&dc);
    qp_driver *drv;
    qp_config cc = { .nqueues = queues, .depth = depth };
    if (!dev || qp_open(&drv, qp_dev_hw(dev), &cc)) { fprintf(stderr, "setup failed\n"); return 1; }

    unsigned window = (depth - 1) * queues / threads;
    if (window > 64) window = 64;
    if (!window) window = 1;
    pthread_t th[64]; bt_t bt[64]; atomic_int errs = 0;
    if (threads > 64) threads = 64;
    uint64_t t0 = now_ns();
    for (unsigned i = 0; i < threads; i++) {
        bt[i] = (bt_t){ .drv = drv, .q = i % queues, .window = window, .seconds = secs, .read_pct = rd,
                        .base_lba = (uint64_t)i * (1u << 16), .rng = 0xABCDEFull + i * 104729u, .errs = &errs };
        pthread_create(&th[i], NULL, worker, &bt[i]);
    }
    uint64_t ops = 0;
    for (unsigned i = 0; i < threads; i++) { pthread_join(th[i], NULL); ops += bt[i].ops; }
    double el = (double)(now_ns() - t0) / 1e9;

    qp_stats s; qp_stats_get(drv, &s);
    printf("threads=%u queues=%u depth=%u window/thread=%u read=%u%% device_lat~%uus\n",
           threads, queues, depth, window, rd, lat);
    printf("ops=%llu  %.0f IOPS  %.1f MiB/s  errors=%d\n", (unsigned long long)ops, (double)ops / el,
           (double)ops * 4096.0 / el / 1048576.0, atomic_load(&errs));
    printf("latency us: p50=%.0f p99=%.0f p99.9=%.0f   max_inflight/queue=%llu resets=%llu\n",
           pct(ops, 0.50), pct(ops, 0.99), pct(ops, 0.999),
           (unsigned long long)s.max_inflight, (unsigned long long)s.resets);
    qp_close(drv);
    qp_dev_destroy(dev);
    return atomic_load(&errs) ? 1 : 0;
}
