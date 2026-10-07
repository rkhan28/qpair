#define _GNU_SOURCE
#include "qp_device.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    uint64_t due_ns, seq;
    uint32_t q;
    qp_sqe   sqe;
} pend_t;

typedef struct {
    bool     active;
    qp_sqe  *sq;
    qp_cqe  *cq;
    uint32_t depth, mask;
    uint32_t sq_head, cq_tail, phase;
} dq_t;

struct qp_device {
    qp_dev_config cfg;
    qp_hw         hw;
    uint8_t      *bar;
    uint8_t      *store;
    pthread_t     th;
    atomic_bool   stop;

    /* owned by the device thread */
    bool     enabled, latched;       /* latched: CFS set, processing stopped */
    dq_t     q[QP_MAX_QUEUES];
    pend_t  *heap;
    size_t   heap_n, heap_cap;
    uint64_t seq, rng;
    uint32_t irq_mask;
    bool     cq_blocked;

    /* cross-thread controls */
    atomic_uint lat_min, lat_max, err_ppm, drop_ppm, drop_next;
    atomic_bool hang, fatal, dead;
    pthread_mutex_t inj_lock;
    struct { uint16_t q, cid; } inj[64];
    unsigned inj_n;

    atomic_uint_least64_t st_fetched, st_completed, st_err, st_drop, st_spur, st_resets;
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t rnd(qp_device *d)  /* xorshift64* */
{
    uint64_t x = d->rng;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    d->rng = x;
    return x * 0x2545F4914F6CDD1Dull;
}

static bool roll(qp_device *d, uint32_t ppm)
{
    return ppm && (rnd(d) % 1000000u) < ppm;
}

static void kick_self(qp_device *d)
{
    uint64_t one = 1;
    ssize_t r = write(d->hw.kick_fd, &one, sizeof one);
    (void)r;
}

/* ---- min-heap of in-flight commands, ordered by (due, seq) ------------- */
static bool heap_less(const pend_t *a, const pend_t *b)
{
    return a->due_ns < b->due_ns || (a->due_ns == b->due_ns && a->seq < b->seq);
}

static void heap_push(qp_device *d, const pend_t *p)
{
    size_t i = d->heap_n++;
    d->heap[i] = *p;
    while (i) {
        size_t parent = (i - 1) / 2;
        if (!heap_less(&d->heap[i], &d->heap[parent])) break;
        pend_t t = d->heap[i]; d->heap[i] = d->heap[parent]; d->heap[parent] = t;
        i = parent;
    }
}

static pend_t heap_pop(qp_device *d)
{
    pend_t top = d->heap[0];
    d->heap[0] = d->heap[--d->heap_n];
    size_t i = 0;
    for (;;) {
        size_t l = 2 * i + 1, r = l + 1, m = i;
        if (l < d->heap_n && heap_less(&d->heap[l], &d->heap[m])) m = l;
        if (r < d->heap_n && heap_less(&d->heap[r], &d->heap[m])) m = r;
        if (m == i) break;
        pend_t t = d->heap[i]; d->heap[i] = d->heap[m]; d->heap[m] = t;
        i = m;
    }
    return top;
}

/* ---- register helpers --------------------------------------------------- */
static uint32_t db_sq_tail(qp_device *d, unsigned q)
{ return qp_ld32(d->bar + QP_DB_BASE + q * QP_DB_STRIDE + QP_DB_SQ_TAIL); }
static uint32_t db_cq_head(qp_device *d, unsigned q)
{ return qp_ld32(d->bar + QP_DB_BASE + q * QP_DB_STRIDE + QP_DB_CQ_HEAD); }

/* The device is only allowed to touch the DMA arena. */
static bool iova_ok(const qp_device *d, uint64_t iova, uint64_t len)
{
    return iova >= QP_ARENA_OFF && iova <= d->hw.bar_size &&
           len <= d->hw.bar_size - iova;
}

static bool pow2(uint32_t v) { return v && !(v & (v - 1)); }

/* ---- enable / disable --------------------------------------------------- */
static bool dev_enable(qp_device *d)
{
    size_t total = 0;
    unsigned nact = 0;

    if (atomic_load(&d->dead)) return false;

    for (unsigned i = 0; i < d->hw.max_queues; i++) {
        uint8_t *r = d->bar + QP_REG_QBASE + i * QP_QREG_STRIDE;
        uint32_t depth = qp_ld32(r + QP_QREG_DEPTH);
        dq_t *q = &d->q[i];
        q->active = false;
        if (!depth) continue;
        uint64_t sqb = qp_ld64(r + QP_QREG_SQ_BASE);
        uint64_t cqb = qp_ld64(r + QP_QREG_CQ_BASE);
        if (!pow2(depth) || depth < 2 || depth > QP_MAX_DEPTH) return false;
        if ((sqb | cqb) & (QP_PAGE - 1)) return false;
        if (!iova_ok(d, sqb, (uint64_t)depth * sizeof(qp_sqe))) return false;
        if (!iova_ok(d, cqb, (uint64_t)depth * sizeof(qp_cqe))) return false;
        q->active = true;
        q->sq = (qp_sqe *)(d->bar + sqb);
        q->cq = (qp_cqe *)(d->bar + cqb);
        q->depth = depth; q->mask = depth - 1;
        q->sq_head = 0; q->cq_tail = 0; q->phase = 1;
        total += depth;
        nact++;
    }
    if (!nact) return false;
    free(d->heap);
    d->heap = calloc(total, sizeof(pend_t));
    if (!d->heap) return false;
    d->heap_cap = total; d->heap_n = 0;
    return true;
}

static void dev_disable(qp_device *d)
{
    d->enabled = d->latched = false;
    d->heap_n = 0;
    d->irq_mask = 0;
    d->cq_blocked = false;
    atomic_store(&d->hang, false);
    atomic_store(&d->fatal, false);
    pthread_mutex_lock(&d->inj_lock);
    d->inj_n = 0;
    pthread_mutex_unlock(&d->inj_lock);
    qp_st32(d->bar + QP_REG_CSTS, 0);   /* last: host may touch queue memory after RDY=0 */
}

static void latch_fatal(qp_device *d)
{
    if (d->latched) return;
    d->latched = true;
    qp_st32(d->bar + QP_REG_CSTS, QP_CSTS_RDY | QP_CSTS_CFS);
    for (unsigned i = 0; i < d->hw.max_queues; i++)
        if (d->q[i].active) d->irq_mask |= 1u << i;
}

static void handle_cc(qp_device *d)
{
    bool en = qp_ld32(d->bar + QP_REG_CC) & QP_CC_EN;
    if (en && !d->enabled) {
        if (dev_enable(d)) {
            d->enabled = true;
            qp_st32(d->bar + QP_REG_CSTS, QP_CSTS_RDY);
        } else {
            qp_st32(d->bar + QP_REG_CSTS, QP_CSTS_CFS);   /* refuses to come up */
        }
    } else if (!en && (d->enabled || (qp_ld32(d->bar + QP_REG_CSTS) & QP_CSTS_CFS))) {
        dev_disable(d);
        atomic_fetch_add(&d->st_resets, 1);
    }
}

/* ---- command execution -------------------------------------------------- */
static uint32_t exec_cmd(qp_device *d, const qp_sqe *s)
{
    switch (s->opc) {
    case QP_OP_FLUSH:
        return QP_SC_OK;
    case QP_OP_READ:
    case QP_OP_WRITE: {
        if (!s->nblk || s->nblk > QP_MAX_XFER) return QP_SC_INVALID_FIELD;
        if (s->lba >= d->cfg.capacity_blocks ||
            s->nblk > d->cfg.capacity_blocks - s->lba) return QP_SC_LBA_RANGE;
        uint64_t len = (uint64_t)s->nblk * QP_BLOCK_SIZE;
        if (!iova_ok(d, s->iova, len)) return QP_SC_DMA_ERROR;
        uint8_t *media = d->store + s->lba * QP_BLOCK_SIZE;
        uint8_t *host  = d->bar + s->iova;
        if (s->opc == QP_OP_READ) memcpy(host, media, len);
        else                      memcpy(media, host, len);
        return QP_SC_OK;
    }
    default:
        return QP_SC_INVALID_OPC;
    }
}

static bool cq_full(qp_device *d, unsigned qi)
{
    dq_t *q = &d->q[qi];
    return ((q->cq_tail + 1) & q->mask) == db_cq_head(d, qi);
}

static void post_cqe(qp_device *d, unsigned qi, uint16_t cid, uint32_t status, uint32_t result)
{
    dq_t *q = &d->q[qi];
    qp_cqe *c = &q->cq[q->cq_tail];
    c->result  = result;
    c->rsvd    = 0;
    c->sq_head = (uint16_t)q->sq_head;
    c->sq_id   = (uint16_t)qi;
    qp_st32(&c->dw3, QP_CQE_DW3(cid, q->phase, status));   /* publish */
    q->cq_tail = (q->cq_tail + 1) & q->mask;
    if (q->cq_tail == 0) q->phase ^= 1;
    d->irq_mask |= 1u << qi;
}

static unsigned pick_latency_us(qp_device *d)
{
    unsigned lo = atomic_load(&d->lat_min), hi = atomic_load(&d->lat_max);
    if (hi <= lo) return lo;
    return lo + (unsigned)(rnd(d) % (hi - lo + 1));
}

static void fetch_all(qp_device *d, uint64_t now)
{
    for (unsigned i = 0; i < d->hw.max_queues && !d->latched; i++) {
        dq_t *q = &d->q[i];
        if (!q->active) continue;
        uint32_t tail = db_sq_tail(d, i);
        if (tail >= q->depth) { latch_fatal(d); return; }     /* driver bug */
        while (q->sq_head != tail) {
            if (d->heap_n == d->heap_cap) { latch_fatal(d); return; }
            pend_t p;
            memcpy(&p.sqe, &q->sq[q->sq_head], sizeof p.sqe);
            q->sq_head = (q->sq_head + 1) & q->mask;
            p.due_ns = now + (uint64_t)pick_latency_us(d) * 1000u;
            p.seq = d->seq++;
            p.q = i;
            heap_push(d, &p);
            atomic_fetch_add(&d->st_fetched, 1);
        }
    }
}

static bool take_drop_next(qp_device *d)
{
    unsigned n = atomic_load(&d->drop_next);
    while (n && !atomic_compare_exchange_weak(&d->drop_next, &n, n - 1)) { }
    return n > 0;
}

static void complete_due(qp_device *d, uint64_t now)
{
    d->cq_blocked = false;
    while (d->heap_n && d->heap[0].due_ns <= now) {
        /* Never overwrite a CQE the host has not consumed yet. If the CQ is
         * full, leave the command in the heap and retry shortly; the driver
         * bounds in-flight work so this only triggers on a misbehaving host. */
        if (cq_full(d, d->heap[0].q)) { d->cq_blocked = true; break; }
        pend_t p = heap_pop(d);
        uint32_t status, result = 0;
        if (roll(d, atomic_load(&d->err_ppm))) {
            status = QP_SC_INTERNAL;
            atomic_fetch_add(&d->st_err, 1);
        } else {
            status = exec_cmd(d, &p.sqe);
        }
        /* Dropped *after* executing: the work happened, the host never hears
         * about it. Retried writes must therefore be idempotent. */
        if (take_drop_next(d) || roll(d, atomic_load(&d->drop_ppm))) {
            atomic_fetch_add(&d->st_drop, 1);
            continue;
        }
        post_cqe(d, p.q, p.sqe.cid, status, result);
        atomic_fetch_add(&d->st_completed, 1);
    }
}

static void drain_injections(qp_device *d)
{
    pthread_mutex_lock(&d->inj_lock);
    for (unsigned i = 0; i < d->inj_n; i++) {
        unsigned qi = d->inj[i].q;
        if (qi < d->hw.max_queues && d->q[qi].active && !cq_full(d, qi)) {
            post_cqe(d, qi, d->inj[i].cid, QP_SC_OK, 0);
            atomic_fetch_add(&d->st_spur, 1);
        }
    }
    d->inj_n = 0;
    pthread_mutex_unlock(&d->inj_lock);
}

static void flush_irqs(qp_device *d)
{
    for (unsigned i = 0; i < d->hw.max_queues; i++) {
        if (d->irq_mask & (1u << i)) {
            uint64_t one = 1;
            ssize_t r = write(d->hw.irq_fd[i], &one, sizeof one);
            (void)r;
        }
    }
    d->irq_mask = 0;
}

/*
 * Device loop. Each wakeup does, in order: honour CC (enable/reset), pull new
 * SQEs off every queue into a latency heap, then post whatever is due. ppoll()
 * is used instead of poll() because the latency model is in microseconds and
 * poll's millisecond timeout would round every command up to 1 ms.
 */
static void *dev_main(void *arg)
{
    qp_device *d = arg;
    struct pollfd pfd = { .fd = d->hw.kick_fd, .events = POLLIN };

    while (!atomic_load(&d->stop)) {
        bool run = d->enabled && !d->latched && !atomic_load(&d->hang);
        struct timespec ts, *tsp = NULL;
        if (run) {
            int64_t wait_ns = -1;
            if (d->cq_blocked) wait_ns = 1000000;
            else if (d->heap_n) {
                int64_t dt = (int64_t)d->heap[0].due_ns - (int64_t)now_ns();
                wait_ns = dt > 0 ? dt : 0;
            }
            if (wait_ns >= 0) {
                ts.tv_sec = wait_ns / 1000000000; ts.tv_nsec = wait_ns % 1000000000;
                tsp = &ts;
            }
        }
        if (ppoll(&pfd, 1, tsp, NULL) > 0 && (pfd.revents & POLLIN)) {
            uint64_t v;
            ssize_t r = read(d->hw.kick_fd, &v, sizeof v);
            (void)r;
        }
        if (atomic_load(&d->stop)) break;

        handle_cc(d);
        if (d->enabled && atomic_exchange(&d->fatal, false)) latch_fatal(d);
        if (d->enabled && !d->latched && !atomic_load(&d->hang)) {
            uint64_t now = now_ns();
            drain_injections(d);
            fetch_all(d, now);
            complete_due(d, now_ns());
        }
        flush_irqs(d);
    }
    return NULL;
}

/* ---- public API --------------------------------------------------------- */
qp_device *qp_dev_create(const qp_dev_config *in)
{
    qp_device *d = calloc(1, sizeof *d);
    if (!d) return NULL;
    if (in) d->cfg = *in;
    if (!d->cfg.max_queues || d->cfg.max_queues > QP_MAX_QUEUES) d->cfg.max_queues = 8;
    if (!d->cfg.capacity_blocks) d->cfg.capacity_blocks = 65536;
    if (!d->cfg.arena_bytes) d->cfg.arena_bytes = 16u << 20;
    d->cfg.arena_bytes = (d->cfg.arena_bytes + QP_PAGE - 1) & ~(size_t)(QP_PAGE - 1);
    d->rng = d->cfg.seed ? d->cfg.seed : 0x9E3779B97F4A7C15ull;

    d->hw.bar_size = QP_ARENA_OFF + d->cfg.arena_bytes;
    d->bar = mmap(NULL, d->hw.bar_size, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (d->bar == MAP_FAILED) { free(d); return NULL; }
    d->store = calloc(d->cfg.capacity_blocks, QP_BLOCK_SIZE);
    d->hw.bar = d->bar;
    d->hw.max_queues = d->cfg.max_queues;
    d->hw.kick_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    for (unsigned i = 0; i < QP_MAX_QUEUES; i++) d->hw.irq_fd[i] = -1;
    for (unsigned i = 0; i < d->hw.max_queues; i++)
        d->hw.irq_fd[i] = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

    atomic_init(&d->lat_min, d->cfg.lat_min_us);
    atomic_init(&d->lat_max, d->cfg.lat_max_us);
    atomic_init(&d->err_ppm, 0); atomic_init(&d->drop_ppm, 0); atomic_init(&d->drop_next, 0);
    atomic_init(&d->hang, false); atomic_init(&d->fatal, false); atomic_init(&d->dead, false);
    atomic_init(&d->stop, false);
    pthread_mutex_init(&d->inj_lock, NULL);

    qp_st64(d->bar + QP_REG_CAP, QP_MAX_DEPTH | ((uint64_t)d->hw.max_queues << 16));
    qp_st64(d->bar + QP_REG_NBLOCKS, d->cfg.capacity_blocks);
    qp_st32(d->bar + QP_REG_BLKSZ, QP_BLOCK_SIZE);

    if (!d->store || d->hw.kick_fd < 0 || pthread_create(&d->th, NULL, dev_main, d)) {
        qp_dev_destroy(d);
        return NULL;
    }
    return d;
}

void qp_dev_destroy(qp_device *d)
{
    if (!d) return;
    if (d->th) {
        atomic_store(&d->stop, true);
        kick_self(d);
        pthread_join(d->th, NULL);
    }
    for (unsigned i = 0; i < QP_MAX_QUEUES; i++)
        if (d->hw.irq_fd[i] >= 0) close(d->hw.irq_fd[i]);
    if (d->hw.kick_fd >= 0) close(d->hw.kick_fd);
    if (d->bar && d->bar != MAP_FAILED) munmap(d->bar, d->hw.bar_size);
    pthread_mutex_destroy(&d->inj_lock);
    free(d->heap);
    free(d->store);
    free(d);
}

const qp_hw *qp_dev_hw(qp_device *d) { return &d->hw; }

void qp_dev_stats_get(qp_device *d, qp_dev_stats *o)
{
    o->fetched = atomic_load(&d->st_fetched);
    o->completed = atomic_load(&d->st_completed);
    o->errors_injected = atomic_load(&d->st_err);
    o->dropped = atomic_load(&d->st_drop);
    o->spurious = atomic_load(&d->st_spur);
    o->resets = atomic_load(&d->st_resets);
}

void qp_dev_set_latency(qp_device *d, unsigned lo, unsigned hi)
{ atomic_store(&d->lat_min, lo); atomic_store(&d->lat_max, hi); kick_self(d); }
void qp_dev_set_error_ppm(qp_device *d, uint32_t ppm) { atomic_store(&d->err_ppm, ppm); }
void qp_dev_set_drop_ppm(qp_device *d, uint32_t ppm)  { atomic_store(&d->drop_ppm, ppm); }
void qp_dev_drop_next(qp_device *d, unsigned n)       { atomic_store(&d->drop_next, n); }
void qp_dev_hang(qp_device *d, bool on)               { atomic_store(&d->hang, on); kick_self(d); }
void qp_dev_fatal(qp_device *d)                       { atomic_store(&d->fatal, true); kick_self(d); }
void qp_dev_set_dead(qp_device *d, bool on)           { atomic_store(&d->dead, on); }

void qp_dev_inject_spurious(qp_device *d, unsigned qid, uint16_t cid)
{
    pthread_mutex_lock(&d->inj_lock);
    if (d->inj_n < sizeof d->inj / sizeof d->inj[0]) {
        d->inj[d->inj_n].q = (uint16_t)qid;
        d->inj[d->inj_n].cid = cid;
        d->inj_n++;
    }
    pthread_mutex_unlock(&d->inj_lock);
    kick_self(d);
}
