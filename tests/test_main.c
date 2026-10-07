#define _GNU_SOURCE
#include "qp_driver.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
 * Test strategy
 *  - Protocol: phase-bit wraparound, out-of-order completion, backpressure.
 *  - Contract: malformed commands must fail with the right errno and must not
 *    be retried or take the device down.
 *  - Recovery: each failure mode the driver claims to survive (transient error,
 *    lost completion, hang, fatal status, dead device, forged completion) has
 *    its own test, plus shutdown with commands still in flight.
 *  - Stress: many threads over shared queues, each verifying its own region
 *    against a shadow copy, with and without injected faults.
 * Sanitizer builds set QP_TEST_SCALE to shrink loops and stretch timeouts.
 */
static atomic_int g_fail;
#define CHECK(c) do { if (!(c)) { atomic_fetch_add(&g_fail, 1); \
    fprintf(stderr, "    CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); } } while (0)
#define REQUIRE(c) do { if (!(c)) { \
    fprintf(stderr, "    REQUIRE failed %s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)

static int scale(void)  /* sanitizer builds are slower: stretch timeouts, shrink loops */
{
    const char *s = getenv("QP_TEST_SCALE");
    return s ? atoi(s) : 1;
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

static uint64_t mix(uint64_t x)
{
    x ^= x >> 33; x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ull;
    x ^= x >> 33; return x;
}
static void fill(void *p, size_t n, uint64_t seed)
{
    for (size_t i = 0; i + 8 <= n; i += 8) { uint64_t v = mix(seed * 0x9E3779B97F4A7C15ull + i); memcpy((uint8_t *)p + i, &v, 8); }
}

typedef struct { qp_device *dev; qp_driver *drv; } env_t;

static void env_up(env_t *e, qp_dev_config dc, qp_config cc)
{
    e->dev = qp_dev_create(&dc);
    REQUIRE(e->dev);
    int rc = qp_open(&e->drv, qp_dev_hw(e->dev), &cc);
    if (rc) fprintf(stderr, "    qp_open: %d\n", rc);
    REQUIRE(rc == 0);
}
static void env_down(env_t *e) { qp_close(e->drv); qp_dev_destroy(e->dev); }

static int io(env_t *e, unsigned q, int opc, uint64_t lba, uint32_t nblk, const qp_dma *b)
{
    qp_cmd c = { .opc = (uint8_t)opc, .lba = lba, .nblk = nblk, .iova = b ? b->iova : 0 };
    return qp_exec(e->drv, q, &c, NULL);
}
static qp_dma dma(env_t *e, size_t n) { qp_dma m; REQUIRE(qp_dma_alloc(e->drv, n, &m) == 0); return m; }

/* ---- batch helper: wait for N async completions ----------------------- */
typedef struct { pthread_mutex_t m; pthread_cond_t c; int pending, bad, seq; } batch_t;
typedef struct { batch_t *b; int status, order; } breq_t;

static void batch_init(batch_t *b) { memset(b, 0, sizeof *b); pthread_mutex_init(&b->m, NULL); pthread_cond_init(&b->c, NULL); }
static void batch_cb(void *ctx, int st, uint32_t r)
{
    (void)r;
    breq_t *q = ctx; batch_t *b = q->b;
    pthread_mutex_lock(&b->m);
    q->status = st; q->order = b->seq++;
    if (st) b->bad++;
    b->pending--;
    pthread_cond_broadcast(&b->c);
    pthread_mutex_unlock(&b->m);
}
static int batch_submit(env_t *e, batch_t *b, breq_t *r, unsigned q, qp_cmd c)
{
    pthread_mutex_lock(&b->m); b->pending++; pthread_mutex_unlock(&b->m);
    r->b = b;
    int rc = qp_submit(e->drv, q, &c, batch_cb, r);
    if (rc) { pthread_mutex_lock(&b->m); b->pending--; pthread_mutex_unlock(&b->m); }
    return rc;
}
static void batch_wait(batch_t *b)
{
    pthread_mutex_lock(&b->m);
    while (b->pending > 0) pthread_cond_wait(&b->c, &b->m);
    pthread_mutex_unlock(&b->m);
}

/* ======================================================================== */
static void t_basic(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .nqueues = 2, .depth = 16 });
    qp_dma w = dma(&e, 4096), r = dma(&e, 4096);
    fill(w.va, 4096, 1);
    CHECK(io(&e, 0, QP_OP_WRITE, 100, 8, &w) == 0);
    memset(r.va, 0, 4096);
    CHECK(io(&e, 1, QP_OP_READ, 100, 8, &r) == 0);          /* different queue */
    CHECK(memcmp(w.va, r.va, 4096) == 0);
    CHECK(io(&e, 0, QP_OP_FLUSH, 0, 0, NULL) == 0);
    CHECK(io(&e, 7, QP_OP_FLUSH, 0, 0, NULL) == -EINVAL);   /* no such queue */
    qp_dma_free(e.drv, &w); qp_dma_free(e.drv, &r);
    env_down(&e);
}

static void t_wraparound(void)
{
    /* depth 8 => the CQ phase bit flips every 8 completions */
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .depth = 8 });
    qp_dma w = dma(&e, 512), r = dma(&e, 512);
    int n = 6000 / scale(), bad = 0;
    for (int i = 0; i < n; i++) {
        fill(w.va, 512, (uint64_t)i);
        uint64_t lba = (uint64_t)i % 64;
        if (io(&e, 0, QP_OP_WRITE, lba, 1, &w) || io(&e, 0, QP_OP_READ, lba, 1, &r) ||
            memcmp(w.va, r.va, 512)) bad++;
    }
    CHECK(bad == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.completed == (uint64_t)n * 2);
    CHECK(s.spurious == 0 && s.resets == 0);
    qp_dma_free(e.drv, &w); qp_dma_free(e.drv, &r);
    env_down(&e);
}

static void t_out_of_order(void)
{
    env_t e; env_up(&e, (qp_dev_config){ .lat_min_us = 0, .lat_max_us = 2000 }, (qp_config){ .depth = 128 });
    enum { N = 64 };
    qp_dma buf = dma(&e, N * 512), rb = dma(&e, N * 512);
    batch_t b; batch_init(&b);
    breq_t rq[N];
    for (int i = 0; i < N; i++) {
        fill((uint8_t *)buf.va + i * 512, 512, 1000u + (unsigned)i);
        qp_cmd c = { QP_OP_WRITE, (uint64_t)i, 1, buf.iova + (uint64_t)i * 512 };
        REQUIRE(batch_submit(&e, &b, &rq[i], 0, c) == 0);
    }
    batch_wait(&b);
    int inversions = 0;
    for (int i = 0; i < N; i++) if (rq[i].order != i) inversions++;
    CHECK(b.bad == 0);
    CHECK(inversions > 0);                                  /* completions really arrive out of order */
    CHECK(io(&e, 0, QP_OP_READ, 0, N, &rb) == 0);
    CHECK(memcmp(buf.va, rb.va, N * 512) == 0);
    qp_dma_free(e.drv, &buf); qp_dma_free(e.drv, &rb);
    env_down(&e);
}

static void t_backpressure(void)
{
    env_t e; env_up(&e, (qp_dev_config){ .lat_min_us = 200, .lat_max_us = 400 }, (qp_config){ .depth = 4 });
    enum { N = 100 };
    qp_dma buf = dma(&e, N * 512), rb = dma(&e, N * 512);
    batch_t b; batch_init(&b);
    static breq_t rq[N];
    for (int i = 0; i < N; i++) {
        fill((uint8_t *)buf.va + i * 512, 512, 7000u + (unsigned)i);
        qp_cmd c = { QP_OP_WRITE, (uint64_t)i, 1, buf.iova + (uint64_t)i * 512 };
        REQUIRE(batch_submit(&e, &b, &rq[i], 0, c) == 0);   /* blocks when the ring is full */
    }
    batch_wait(&b);
    CHECK(b.bad == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.max_inflight <= 3);                             /* depth-1: ring can never overflow */
    CHECK(s.max_inflight >= 2);
    CHECK(io(&e, 0, QP_OP_READ, 0, N > 256 ? 256 : N, &rb) == 0);
    CHECK(memcmp(buf.va, rb.va, N * 512) == 0);
    qp_dma_free(e.drv, &buf); qp_dma_free(e.drv, &rb);
    env_down(&e);
}

static void t_bad_commands(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){0});
    const qp_hw *hw = qp_dev_hw(e.dev);
    qp_dma b = dma(&e, 4096);
    struct { qp_cmd c; int want; const char *name; } t[] = {
        { { 0x7F, 0, 1, b.iova },                 -ENOTSUP, "bad opcode" },
        { { QP_OP_READ, 0, 0, b.iova },           -EINVAL,  "zero length" },
        { { QP_OP_READ, 0, 257, b.iova },         -EINVAL,  "over max transfer" },
        { { QP_OP_READ, 65536, 1, b.iova },       -ERANGE,  "lba past end" },
        { { QP_OP_WRITE, 65535, 2, b.iova },      -ERANGE,  "range straddles end" },
        { { QP_OP_READ, 0, 1, 0x100 },            -EFAULT,  "iova below arena" },
        { { QP_OP_READ, 0, 1, hw->bar_size - 100 }, -EFAULT, "dma runs past arena" },
        { { QP_OP_READ, 0, 1, UINT64_MAX - 10 },  -EFAULT,  "iova overflow" },
    };
    for (size_t i = 0; i < sizeof t / sizeof t[0]; i++) {
        int rc = qp_exec(e.drv, 0, &t[i].c, NULL);
        if (rc != t[i].want) fprintf(stderr, "    %s: got %d want %d\n", t[i].name, rc, t[i].want);
        CHECK(rc == t[i].want);
    }
    CHECK(io(&e, 0, QP_OP_WRITE, 5, 1, &b) == 0);           /* device still healthy */
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.retried == 0 && s.resets == 0);                 /* permanent errors are not retried */
    qp_dma_free(e.drv, &b);
    env_down(&e);
}

static void t_transient_retry(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .nqueues = 2, .depth = 32, .max_retries = 8 });
    qp_dev_set_error_ppm(e.dev, 100000);                    /* 10% of attempts fail */
    qp_dma w = dma(&e, 512), r = dma(&e, 512);
    int n = 1500 / scale(), bad = 0;
    for (int i = 0; i < n; i++) {
        fill(w.va, 512, 50000u + (unsigned)i);
        if (io(&e, (unsigned)i & 1, QP_OP_WRITE, (uint64_t)i % 97, 1, &w) ||
            io(&e, (unsigned)i & 1, QP_OP_READ, (uint64_t)i % 97, 1, &r) ||
            memcmp(w.va, r.va, 512)) bad++;
    }
    CHECK(bad == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.retried > 0);
    CHECK(s.resets == 0);
    qp_dma_free(e.drv, &w); qp_dma_free(e.drv, &r);
    env_down(&e);

    /* a device that always errors must surface -EIO after max_retries */
    env_up(&e, (qp_dev_config){0}, (qp_config){ .max_retries = 2 });
    qp_dev_set_error_ppm(e.dev, 1000000);
    CHECK(io(&e, 0, QP_OP_FLUSH, 0, 0, NULL) == -EIO);
    qp_stats_get(e.drv, &s);
    CHECK(s.retried == 2);
    env_down(&e);
}

static void t_dropped_completion(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .timeout_ms = 50u * (unsigned)scale() });
    qp_dma w = dma(&e, 512), r = dma(&e, 512);
    fill(w.va, 512, 424242);
    qp_dev_drop_next(e.dev, 1);              /* write executes, completion is lost */
    uint64_t t0 = now_ms();
    CHECK(io(&e, 0, QP_OP_WRITE, 9, 1, &w) == 0);           /* timeout -> reset -> repost */
    uint64_t dt = now_ms() - t0;
    CHECK(dt >= 40);
    CHECK(io(&e, 0, QP_OP_READ, 9, 1, &r) == 0);
    CHECK(memcmp(w.va, r.va, 512) == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    qp_dev_stats ds; qp_dev_stats_get(e.dev, &ds);
    CHECK(s.resets == 1 && s.timeouts == 1);
    CHECK(ds.dropped == 1 && ds.resets == 1);
    qp_dma_free(e.drv, &w); qp_dma_free(e.drv, &r);
    env_down(&e);
}

static void t_hang_recovery(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .nqueues = 2, .depth = 64, .timeout_ms = 60u * (unsigned)scale() });
    enum { N = 32 };
    qp_dma buf = dma(&e, N * 512), rb = dma(&e, N * 512);
    batch_t b; batch_init(&b);
    static breq_t rq[N];
    qp_dev_hang(e.dev, true);                                /* firmware wedged */
    for (int i = 0; i < N; i++) {
        fill((uint8_t *)buf.va + i * 512, 512, 900u + (unsigned)i);
        qp_cmd c = { QP_OP_WRITE, (uint64_t)i, 1, buf.iova + (uint64_t)i * 512 };
        REQUIRE(batch_submit(&e, &b, &rq[i], (unsigned)i & 1, c) == 0);
    }
    batch_wait(&b);
    CHECK(b.bad == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.resets >= 1 && s.timeouts >= N);
    CHECK(io(&e, 0, QP_OP_READ, 0, N, &rb) == 0);
    CHECK(memcmp(buf.va, rb.va, N * 512) == 0);
    qp_dma_free(e.drv, &buf); qp_dma_free(e.drv, &rb);
    env_down(&e);
}

static void t_fatal_status(void)
{
    /* 5 s command timeout: recovery must come from CSTS.CFS, not the timer */
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .depth = 64, .timeout_ms = 5000 });
    enum { N = 16 };
    qp_dma buf = dma(&e, N * 512);
    batch_t b; batch_init(&b);
    static breq_t rq[N];
    qp_dev_hang(e.dev, true);
    for (int i = 0; i < N; i++) {
        fill((uint8_t *)buf.va + i * 512, 512, 31u + (unsigned)i);
        qp_cmd c = { QP_OP_WRITE, (uint64_t)i, 1, buf.iova + (uint64_t)i * 512 };
        REQUIRE(batch_submit(&e, &b, &rq[i], 0, c) == 0);
    }
    uint64_t t0 = now_ms();
    qp_dev_fatal(e.dev);
    batch_wait(&b);
    uint64_t dt = now_ms() - t0;
    CHECK(b.bad == 0);
    CHECK(dt < 1500);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.cfs_events >= 1 && s.resets >= 1 && s.timeouts == 0);
    qp_dma_free(e.drv, &buf);
    env_down(&e);
}

static void t_dead_device(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .timeout_ms = 40, .hw_timeout_ms = 100 });
    enum { N = 4 };
    batch_t b; batch_init(&b);
    static breq_t rq[N];
    qp_dev_set_dead(e.dev, true);                            /* reset can no longer revive it */
    qp_dev_hang(e.dev, true);
    for (int i = 0; i < N; i++) {
        qp_cmd c = { QP_OP_FLUSH, 0, 0, 0 };
        REQUIRE(batch_submit(&e, &b, &rq[i], 0, c) == 0);
    }
    batch_wait(&b);
    CHECK(b.bad == N);
    for (int i = 0; i < N; i++) CHECK(rq[i].status == -EIO);
    CHECK(io(&e, 0, QP_OP_FLUSH, 0, 0, NULL) == -ENODEV);    /* fails fast afterwards */
    env_down(&e);
}

static void t_spurious(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .depth = 64 });  /* ring must hold all 18 forged CQEs */
    unsigned n = 0;
    for (unsigned g = 0; g < 16; g++) { qp_dev_inject_spurious(e.dev, 0, (uint16_t)((g << 12) | 0)); n++; }
    qp_dev_inject_spurious(e.dev, 0, 0x0FFF); n++;           /* slot index out of range */
    qp_dev_inject_spurious(e.dev, 0, 0x0020); n++;
    for (int i = 0; i < 400 && ({ qp_stats s; qp_stats_get(e.drv, &s); s.spurious < n; }); i++) usleep(1000);
    qp_stats s; qp_stats_get(e.drv, &s);
    CHECK(s.spurious == n);
    qp_dma w = dma(&e, 512), r = dma(&e, 512);
    fill(w.va, 512, 5);
    CHECK(io(&e, 0, QP_OP_WRITE, 1, 1, &w) == 0);            /* ring/phase still coherent */
    CHECK(io(&e, 0, QP_OP_READ, 1, 1, &r) == 0 && !memcmp(w.va, r.va, 512));
    qp_dma_free(e.drv, &w); qp_dma_free(e.drv, &r);
    env_down(&e);

    /* forged completion naming a BUSY slot with the wrong generation */
    env_up(&e, (qp_dev_config){ .lat_min_us = 50000, .lat_max_us = 50000 }, (qp_config){ .depth = 16 });
    batch_t b; batch_init(&b);
    breq_t rq;
    qp_cmd c = { QP_OP_FLUSH, 0, 0, 0 };
    REQUIRE(batch_submit(&e, &b, &rq, 0, c) == 0);           /* slot 0, generation 1 */
    qp_dev_inject_spurious(e.dev, 0, 0x0000);
    qp_dev_inject_spurious(e.dev, 0, 0x2000);
    qp_dev_inject_spurious(e.dev, 0, 0x3000);
    batch_wait(&b);
    CHECK(rq.status == 0 && b.bad == 0);                     /* real completion arrived intact */
    qp_stats_get(e.drv, &s);
    CHECK(s.spurious == 3 && s.completed == 1);
    env_down(&e);
}

static void t_dma_allocator(void)
{
    env_t e; env_up(&e, (qp_dev_config){ .arena_bytes = 64 * QP_PAGE }, (qp_config){ .depth = 16 });
    qp_dma m[64]; int n = 0;
    while (n < 64 && qp_dma_alloc(e.drv, 1, &m[n]) == 0) n++;
    CHECK(n == 62);                                          /* SQ + CQ took two pages */
    qp_dma tmp;
    CHECK(qp_dma_alloc(e.drv, 1, &tmp) == -ENOMEM);
    for (int i = 0; i < n; i += 2) CHECK(qp_dma_free(e.drv, &m[i]) == 0);
    CHECK(qp_dma_alloc(e.drv, 2 * QP_PAGE, &tmp) == -ENOMEM);  /* fragmented: no 2-page run */
    CHECK(qp_dma_alloc(e.drv, QP_PAGE, &tmp) == 0);
    CHECK(qp_dma_free(e.drv, &tmp) == 0);
    qp_dma dup = m[1];
    CHECK(qp_dma_free(e.drv, &m[1]) == 0);
    CHECK(qp_dma_free(e.drv, &dup) == -EINVAL);              /* double free caught */
    for (int i = 3; i < n; i += 2) CHECK(qp_dma_free(e.drv, &m[i]) == 0);
    CHECK(qp_dma_alloc(e.drv, 62 * QP_PAGE, &tmp) == 0);     /* fully coalesced again */
    CHECK(qp_dma_free(e.drv, &tmp) == 0);
    env_down(&e);
}

static atomic_int g_cancel;
static void cancel_cb(void *ctx, int st, uint32_t r) { (void)ctx; (void)r; if (st == -ECANCELED) atomic_fetch_add(&g_cancel, 1); }

static void t_close_inflight(void)
{
    env_t e; env_up(&e, (qp_dev_config){0}, (qp_config){ .timeout_ms = 60000 });
    qp_dev_hang(e.dev, true);
    atomic_store(&g_cancel, 0);
    for (int i = 0; i < 8; i++) {
        qp_cmd c = { QP_OP_FLUSH, 0, 0, 0 };
        REQUIRE(qp_submit(e.drv, 0, &c, cancel_cb, NULL) == 0);
    }
    env_down(&e);
    CHECK(atomic_load(&g_cancel) == 8);
}

/* ---- multithreaded stress with a shadow copy per thread ------------------ */
enum { REGION = 1024, NTHREADS = 8, NQ = 4 };
typedef struct { env_t *e; int tid, iters; uint64_t rng; int errs, mism; } sthr_t;

static uint64_t rng_next(uint64_t *s) { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; return *s; }

static void *stress_thread(void *arg)
{
    sthr_t *t = arg;
    qp_dma buf; REQUIRE(qp_dma_alloc(t->e->drv, 8 * 512, &buf) == 0);
    uint8_t *shadow = calloc(REGION, 512);
    unsigned q = (unsigned)t->tid % NQ;
    for (int i = 0; i < t->iters; i++) {
        uint64_t r = rng_next(&t->rng);
        unsigned nb = 1 + (unsigned)(r >> 8) % 8;
        unsigned off = (unsigned)(r >> 16) % (REGION - nb);
        uint64_t lba = (uint64_t)t->tid * REGION + off;
        unsigned op = (unsigned)r % 100;
        if (op < 55) {
            fill(buf.va, nb * 512, rng_next(&t->rng));
            if (io(t->e, q, QP_OP_WRITE, lba, nb, &buf)) t->errs++;
            else memcpy(shadow + (size_t)off * 512, buf.va, nb * 512);
        } else if (op < 95) {
            memset(buf.va, 0xEE, nb * 512);
            if (io(t->e, q, QP_OP_READ, lba, nb, &buf)) t->errs++;
            else if (memcmp(shadow + (size_t)off * 512, buf.va, nb * 512)) t->mism++;
        } else {
            if (io(t->e, q, QP_OP_FLUSH, 0, 0, NULL)) t->errs++;
        }
    }
    free(shadow);
    qp_dma_free(t->e->drv, &buf);
    return NULL;
}

static void stress(bool faulty)
{
    int sc = scale();
    env_t e;
    env_up(&e, (qp_dev_config){ .max_queues = NQ, .lat_min_us = 0, .lat_max_us = 80 },
           (qp_config){ .nqueues = NQ, .depth = 32, .timeout_ms = faulty ? 100u * (unsigned)sc : 2000,
                        .max_retries = 6 });
    if (faulty) { qp_dev_set_error_ppm(e.dev, 20000); qp_dev_set_drop_ppm(e.dev, 1000); }
    pthread_t th[NTHREADS]; sthr_t st[NTHREADS];
    int iters = (faulty ? 2000 : 4000) / sc;
    for (int i = 0; i < NTHREADS; i++) {
        st[i] = (sthr_t){ .e = &e, .tid = i, .iters = iters, .rng = 0x1234567ull + (uint64_t)i * 7919u };
        pthread_create(&th[i], NULL, stress_thread, &st[i]);
    }
    int errs = 0, mism = 0;
    for (int i = 0; i < NTHREADS; i++) { pthread_join(th[i], NULL); errs += st[i].errs; mism += st[i].mism; }
    CHECK(errs == 0);
    CHECK(mism == 0);
    qp_stats s; qp_stats_get(e.drv, &s);
    qp_dev_stats ds; qp_dev_stats_get(e.dev, &ds);
    fprintf(stderr, "    ops=%llu retried=%llu resets=%llu timeouts=%llu dev_dropped=%llu\n",
            (unsigned long long)s.completed, (unsigned long long)s.retried, (unsigned long long)s.resets,
            (unsigned long long)s.timeouts, (unsigned long long)ds.dropped);
    CHECK(s.completed == s.submitted);
    CHECK(s.max_inflight <= 31);
    if (faulty) { CHECK(s.retried > 0); CHECK(ds.dropped == 0 || s.resets > 0); }
    else        { CHECK(s.retried == 0 && s.resets == 0 && s.spurious == 0); }
    env_down(&e);
}
static void t_stress_clean(void)  { stress(false); }
static void t_stress_faulty(void) { stress(true); }

/* ======================================================================== */
#define RUN(fn) do { uint64_t t0 = now_ms(); int f0 = atomic_load(&g_fail); \
    fprintf(stderr, "[ RUN  ] %s\n", #fn); fn(); \
    fprintf(stderr, "[ %s ] %s (%llu ms)\n", atomic_load(&g_fail) == f0 ? " OK " : "FAIL", #fn, \
            (unsigned long long)(now_ms() - t0)); } while (0)

int main(void)
{
    RUN(t_basic);
    RUN(t_wraparound);
    RUN(t_out_of_order);
    RUN(t_backpressure);
    RUN(t_bad_commands);
    RUN(t_transient_retry);
    RUN(t_dropped_completion);
    RUN(t_hang_recovery);
    RUN(t_fatal_status);
    RUN(t_dead_device);
    RUN(t_spurious);
    RUN(t_dma_allocator);
    RUN(t_close_inflight);
    RUN(t_stress_clean);
    RUN(t_stress_faulty);
    int f = atomic_load(&g_fail);
    fprintf(stderr, "\n%s (%d failed checks)\n", f ? "FAILED" : "ALL TESTS PASSED", f);
    return f ? 1 : 0;
}
