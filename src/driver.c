#define _GNU_SOURCE
#include "qp_driver.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

/*
 * Design notes
 *
 * Locking: one mutex per queue guards the SQ tail, the free-slot list and the
 * slot table. The reaper thread takes it briefly per completion and always
 * drops it before running a user callback, so a callback may call back into
 * the driver without self-deadlocking on the queue lock.
 *
 * Capacity: a queue of depth N has N-1 command slots, the same rule NVMe uses
 * (head == tail means "empty", so a completely full ring is indistinguishable
 * from an empty one). Tying slots to ring space means submit() only needs the
 * free-slot list for backpressure; the SQ can never overflow.
 *
 * Recovery: the device is treated as something that can silently lose work.
 * Every command carries a deadline; an expired deadline or CSTS.CFS triggers
 * a full controller reset and the survivors are re-posted. That is only safe
 * because every opcode here (read, write, flush) is idempotent.
 */

/*
 * Command identifiers: cid = (generation << 12) | slot. The generation bumps
 * every time a slot is reused, so a stale or forged completion that names a
 * slot which is idle -- or busy with a *different* command -- is rejected
 * instead of completing the wrong request.
 */
#define CID_SLOT_BITS 12
#define CID_SLOT_MASK ((1u << CID_SLOT_BITS) - 1)
static inline uint16_t make_cid(uint16_t gen, unsigned slot)
{ return (uint16_t)(((gen & 0xFu) << CID_SLOT_BITS) | slot); }

typedef struct {
    qp_cmd      cmd;
    qp_done_fn  cb;
    void       *ctx;
    uint64_t    deadline_ns;
    uint32_t    retries;
    uint16_t    gen;
    bool        inflight;
    int         next_free;
} slot_t;

typedef struct {
    qp_done_fn cb;
    void      *ctx;
    int        status;
    uint32_t   result;
} fin_t;

typedef struct {
    pthread_mutex_t lock;       /* guards everything below except cq_head/cq_phase */
    pthread_cond_t  slot_cv;
    unsigned        id, depth, mask, nslots;
    qp_dma          sq_mem, cq_mem;
    qp_sqe         *sq;
    qp_cqe         *cq;
    void           *sq_db, *cq_db;
    uint32_t        sq_tail, sq_head_seen;
    slot_t         *slots;
    int             free_head;
    unsigned        inflight, max_inflight;
    /* reaper-thread private (also touched by reset, which runs on the reaper) */
    uint32_t        cq_head, cq_phase;
} queue_t;

struct qp_driver {
    const qp_hw *hw;
    qp_config    cfg;
    unsigned     nq;
    size_t       total_slots;
    queue_t      q[QP_MAX_QUEUES];

    pthread_mutex_t dma_lock;
    uint8_t        *page_used;
    size_t          npages;

    int         epfd, stop_fd;
    pthread_t   reaper;
    bool        reaper_started;
    atomic_bool dead, stopping;

    atomic_uint_least64_t st_submitted, st_completed, st_retried, st_timeouts,
                          st_resets, st_cfs, st_spurious;
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ---- BAR access --------------------------------------------------------- */
static uint8_t *bar(qp_driver *d) { return d->hw->bar; }
static uint32_t rd32(qp_driver *d, unsigned off) { return qp_ld32(bar(d) + off); }
static void     wr32(qp_driver *d, unsigned off, uint32_t v) { qp_st32(bar(d) + off, v); }
static void     wr64(qp_driver *d, unsigned off, uint64_t v) { qp_st64(bar(d) + off, v); }

static void kick(qp_driver *d)
{
    uint64_t one = 1;
    ssize_t r = write(d->hw->kick_fd, &one, sizeof one);
    (void)r;
}

static int wait_csts(qp_driver *d, uint32_t mask, uint32_t want)
{
    uint64_t end = now_ns() + (uint64_t)d->cfg.hw_timeout_ms * 1000000ull;
    for (;;) {
        if ((rd32(d, QP_REG_CSTS) & mask) == want) return 0;
        if (now_ns() >= end) return -ETIMEDOUT;
        usleep(50);
    }
}

static int hw_disable(qp_driver *d)
{
    wr32(d, QP_REG_CC, 0);
    kick(d);
    return wait_csts(d, QP_CSTS_RDY, 0);
}

static int hw_enable(qp_driver *d)
{
    for (unsigned i = 0; i < d->hw->max_queues; i++) {
        unsigned base = QP_REG_QBASE + i * QP_QREG_STRIDE;
        if (i < d->nq) {
            wr64(d, base + QP_QREG_SQ_BASE, d->q[i].sq_mem.iova);
            wr64(d, base + QP_QREG_CQ_BASE, d->q[i].cq_mem.iova);
            wr32(d, base + QP_QREG_DEPTH, d->q[i].depth);
        } else {
            wr32(d, base + QP_QREG_DEPTH, 0);
        }
    }
    wr32(d, QP_REG_CC, QP_CC_EN);
    kick(d);
    return wait_csts(d, QP_CSTS_RDY | QP_CSTS_CFS, QP_CSTS_RDY);
}

/* ---- DMA arena: page-granular first-fit allocator ----------------------- */
int qp_dma_alloc(qp_driver *d, size_t len, qp_dma *out)
{
    if (!len || !out) return -EINVAL;
    size_t need = (len + QP_PAGE - 1) / QP_PAGE;
    int rc = -ENOMEM;
    pthread_mutex_lock(&d->dma_lock);
    size_t run = 0;
    for (size_t i = 0; i < d->npages; i++) {
        run = d->page_used[i] ? 0 : run + 1;
        if (run == need) {
            size_t first = i + 1 - need;
            memset(d->page_used + first, 1, need);
            out->iova = QP_ARENA_OFF + first * QP_PAGE;
            out->va   = bar(d) + out->iova;
            out->len  = need * QP_PAGE;
            rc = 0;
            break;
        }
    }
    pthread_mutex_unlock(&d->dma_lock);
    return rc;
}

int qp_dma_free(qp_driver *d, qp_dma *m)
{
    if (!m || !m->va) return -EINVAL;
    size_t first = (m->iova - QP_ARENA_OFF) / QP_PAGE, n = m->len / QP_PAGE;
    int rc = 0;
    pthread_mutex_lock(&d->dma_lock);
    if (first + n > d->npages) rc = -EINVAL;
    for (size_t i = 0; !rc && i < n; i++)
        if (!d->page_used[first + i]) rc = -EINVAL;      /* double free */
    if (!rc) memset(d->page_used + first, 0, n);
    pthread_mutex_unlock(&d->dma_lock);
    if (!rc) memset(m, 0, sizeof *m);
    return rc;
}

/* ---- queue plumbing ------------------------------------------------------ */
/* Caller must hold q->lock (or be single-threaded in open/close). */
static void queue_reset_state(queue_t *q)
{
    memset(q->sq_mem.va, 0, (size_t)q->depth * sizeof(qp_sqe));
    memset(q->cq_mem.va, 0, (size_t)q->depth * sizeof(qp_cqe));
    q->sq_tail = q->sq_head_seen = 0;
    q->cq_head = 0;
    q->cq_phase = 1;
    qp_st32(q->sq_db, 0);
    qp_st32(q->cq_db, 0);
}

static void post_locked(queue_t *q, unsigned idx)
{
    slot_t *s = &q->slots[idx];
    qp_sqe *e = &q->sq[q->sq_tail];
    memset(e, 0, sizeof *e);
    e->opc  = s->cmd.opc;
    e->cid  = make_cid(s->gen, idx);
    e->nsid = 1;
    e->lba  = s->cmd.lba;
    e->nblk = s->cmd.nblk;
    e->iova = s->cmd.iova;
    q->sq_tail = (q->sq_tail + 1) & q->mask;
}

static void ring_sq(qp_driver *d, queue_t *q)
{
    qp_st32(q->sq_db, q->sq_tail);    /* RELEASE: SQE bytes are visible first */
    kick(d);
}

static bool sq_has_room(const queue_t *q)
{
    return ((q->sq_tail + 1) & q->mask) != q->sq_head_seen;
}

static void free_slot_locked(queue_t *q, unsigned idx)
{
    slot_t *s = &q->slots[idx];
    s->inflight = false;
    s->next_free = q->free_head;
    q->free_head = (int)idx;
    q->inflight--;
}

static int map_status(uint32_t sc)
{
    switch (sc) {
    case QP_SC_OK:            return 0;
    case QP_SC_INVALID_OPC:   return -ENOTSUP;
    case QP_SC_INVALID_FIELD: return -EINVAL;
    case QP_SC_LBA_RANGE:     return -ERANGE;
    case QP_SC_DMA_ERROR:     return -EFAULT;
    default:                  return -EIO;
    }
}

/* ---- submit -------------------------------------------------------------- */
int qp_submit(qp_driver *d, unsigned qid, const qp_cmd *cmd, qp_done_fn cb, void *ctx)
{
    if (qid >= d->nq || !cmd || !cb) return -EINVAL;
    queue_t *q = &d->q[qid];

    pthread_mutex_lock(&q->lock);
    while (!atomic_load(&d->dead) && !atomic_load(&d->stopping) &&
           (q->free_head < 0 || !sq_has_room(q)))
        pthread_cond_wait(&q->slot_cv, &q->lock);       /* backpressure */
    if (atomic_load(&d->dead) || atomic_load(&d->stopping)) {
        int rc = atomic_load(&d->dead) ? -ENODEV : -ECANCELED;
        pthread_mutex_unlock(&q->lock);
        return rc;
    }

    /* Both conditions were checked in the wait loop above, under the lock,
     * so a slot and ring space are guaranteed to exist here. */
    unsigned idx = (unsigned)q->free_head;
    slot_t *s = &q->slots[idx];
    q->free_head = s->next_free;
    s->cmd = *cmd;
    s->cb = cb;
    s->ctx = ctx;
    s->retries = 0;
    s->gen = (uint16_t)((s->gen + 1) & 0xF);
    s->deadline_ns = now_ns() + (uint64_t)d->cfg.timeout_ms * 1000000ull;
    s->inflight = true;
    if (++q->inflight > q->max_inflight) q->max_inflight = q->inflight;

    post_locked(q, idx);
    ring_sq(d, q);
    pthread_mutex_unlock(&q->lock);
    atomic_fetch_add(&d->st_submitted, 1);
    return 0;
}

typedef struct {
    pthread_mutex_t m;
    pthread_cond_t  c;
    bool            done;
    int             status;
    uint32_t        result;
} waiter_t;

static void waiter_done(void *ctx, int status, uint32_t result)
{
    waiter_t *w = ctx;
    pthread_mutex_lock(&w->m);
    w->status = status; w->result = result; w->done = true;
    pthread_cond_signal(&w->c);
    pthread_mutex_unlock(&w->m);
}

int qp_exec(qp_driver *d, unsigned qid, const qp_cmd *cmd, uint32_t *result)
{
    waiter_t w = { .m = PTHREAD_MUTEX_INITIALIZER, .c = PTHREAD_COND_INITIALIZER };
    int rc = qp_submit(d, qid, cmd, waiter_done, &w);
    if (rc) return rc;
    pthread_mutex_lock(&w.m);
    while (!w.done) pthread_cond_wait(&w.c, &w.m);
    pthread_mutex_unlock(&w.m);
    if (result) *result = w.result;
    int st = w.status;
    pthread_mutex_destroy(&w.m);
    pthread_cond_destroy(&w.c);
    return st;
}

/* ---- completion path (reaper thread) -------------------------------------- */
static void reap_queue(qp_driver *d, queue_t *q)
{
    unsigned consumed = 0;
    for (;;) {
        qp_cqe *c = &q->cq[q->cq_head];
        uint32_t dw3 = qp_ld32(&c->dw3);                       /* ACQUIRE */
        if (QP_CQE_PHASE(dw3) != q->cq_phase) break;            /* nothing new */

        uint16_t cid = QP_CQE_CID(dw3);
        uint32_t status = QP_CQE_STATUS(dw3);
        uint32_t result = c->result;
        uint16_t sqh = c->sq_head;
        q->cq_head = (q->cq_head + 1) & q->mask;
        if (q->cq_head == 0) q->cq_phase ^= 1;
        consumed++;

        unsigned idx = cid & CID_SLOT_MASK;
        uint16_t gen = (uint16_t)(cid >> CID_SLOT_BITS);
        fin_t fin;
        bool done = false;

        pthread_mutex_lock(&q->lock);
        slot_t *s = idx < q->nslots ? &q->slots[idx] : NULL;
        if (!s || !s->inflight || s->gen != gen) {
            atomic_fetch_add(&d->st_spurious, 1);               /* forged / stale */
        } else {
            if (sqh < q->depth) q->sq_head_seen = sqh;
            /* Transient device error: re-post under the SAME cid. The device
             * already completed the previous attempt, so nothing else is
             * using that cid, and keeping it means the caller's slot,
             * generation and deadline bookkeeping stay valid. */
            if (status == QP_SC_INTERNAL && s->retries < d->cfg.max_retries) {
                s->retries++;                                    /* transient: retry in place */
                s->deadline_ns = now_ns() + (uint64_t)d->cfg.timeout_ms * 1000000ull;
                post_locked(q, idx);
                ring_sq(d, q);
                atomic_fetch_add(&d->st_retried, 1);
            } else {
                fin = (fin_t){ s->cb, s->ctx, map_status(status), result };
                free_slot_locked(q, idx);
                pthread_cond_signal(&q->slot_cv);
                done = true;
            }
        }
        pthread_mutex_unlock(&q->lock);

        if (done) {
            atomic_fetch_add(&d->st_completed, 1);
            fin.cb(fin.ctx, fin.status, fin.result);            /* never under a lock */
        }
    }
    if (consumed) qp_st32(q->cq_db, q->cq_head);                /* tell device CQ space is free */
}

static void reap_all(qp_driver *d)
{
    for (unsigned i = 0; i < d->nq; i++) reap_queue(d, &d->q[i]);
}

static bool any_expired(qp_driver *d, uint64_t now)
{
    bool hit = false;
    for (unsigned i = 0; i < d->nq && !hit; i++) {
        queue_t *q = &d->q[i];
        pthread_mutex_lock(&q->lock);
        for (unsigned j = 0; j < q->nslots; j++)
            if (q->slots[j].inflight && q->slots[j].deadline_ns <= now) { hit = true; break; }
        pthread_mutex_unlock(&q->lock);
    }
    return hit;
}

/* Collect every in-flight command into fins[] and free its slot. Locks held. */
static size_t drain_inflight_locked(qp_driver *d, fin_t *fins, int status)
{
    size_t n = 0;
    for (unsigned i = 0; i < d->nq; i++) {
        queue_t *q = &d->q[i];
        for (unsigned j = 0; j < q->nslots; j++) {
            slot_t *s = &q->slots[j];
            if (!s->inflight) continue;
            fins[n++] = (fin_t){ s->cb, s->ctx, status, 0 };
            free_slot_locked(q, j);
        }
    }
    return n;
}

/*
 * Controller reset. Runs on the reaper thread, with every queue lock held so
 * no submitter can touch a ring while it is torn down:
 *   1. CC.EN=0, wait RDY=0 (device has stopped touching our memory)
 *   2. zero rings, reset head/tail/phase
 *   3. reprogram, CC.EN=1, wait RDY=1
 *   4. re-post every command that was in flight. Commands that themselves had
 *      timed out are charged a retry; innocent bystanders are not.
 */
static void do_reset(qp_driver *d)
{
    uint64_t t0 = now_ns();
    fin_t *fins = malloc(d->total_slots * sizeof *fins);
    size_t nfin = 0;
    bool failed = false;

    for (unsigned i = 0; i < d->nq; i++) pthread_mutex_lock(&d->q[i].lock);
    atomic_fetch_add(&d->st_resets, 1);

    if (!fins || hw_disable(d)) {
        failed = true;
    } else {
        for (unsigned i = 0; i < d->nq; i++) queue_reset_state(&d->q[i]);
        if (hw_enable(d)) failed = true;
    }

    if (failed) {
        atomic_store(&d->dead, true);            /* device is gone: fail everything */
        if (fins) nfin = drain_inflight_locked(d, fins, -EIO);
    } else {
        uint64_t now = now_ns();
        for (unsigned i = 0; i < d->nq; i++) {
            queue_t *q = &d->q[i];
            bool posted = false;
            for (unsigned j = 0; j < q->nslots; j++) {
                slot_t *s = &q->slots[j];
                if (!s->inflight) continue;
                /* Only a command that itself timed out is charged a retry.
                 * One lost completion resets the whole device, and charging
                 * every bystander would let a single bad command exhaust the
                 * retry budget of unrelated I/O. */
                if (s->deadline_ns <= t0) {
                    atomic_fetch_add(&d->st_timeouts, 1);
                    if (++s->retries > d->cfg.max_retries) {
                        fins[nfin++] = (fin_t){ s->cb, s->ctx, -ETIMEDOUT, 0 };
                        free_slot_locked(q, j);
                        continue;
                    }
                }
                s->deadline_ns = now + (uint64_t)d->cfg.timeout_ms * 1000000ull;
                post_locked(q, j);
                posted = true;
            }
            if (posted) ring_sq(d, q);
        }
    }

    for (unsigned i = 0; i < d->nq; i++) {
        pthread_cond_broadcast(&d->q[i].slot_cv);
        pthread_mutex_unlock(&d->q[i].lock);
    }
    for (size_t i = 0; i < nfin; i++) {
        atomic_fetch_add(&d->st_completed, 1);
        fins[i].cb(fins[i].ctx, fins[i].status, fins[i].result);
    }
    free(fins);
}

static void *reaper_main(void *arg)
{
    qp_driver *d = arg;
    struct epoll_event evs[QP_MAX_QUEUES + 1];
    uint64_t last_scan = 0;

    while (!atomic_load(&d->stopping)) {
        /* The 5 ms timeout is the heartbeat for things no interrupt will ever
         * announce: an expired deadline, or a wedged device that stopped
         * raising IRQs. Interrupts still wake the loop immediately. */
        int n = epoll_wait(d->epfd, evs, QP_MAX_QUEUES + 1, 5);
        for (int i = 0; i < n; i++) {
            uint64_t v;
            ssize_t r = read(evs[i].data.fd, &v, sizeof v);
            (void)r;
        }
        if (atomic_load(&d->stopping) || atomic_load(&d->dead)) continue;

        reap_all(d);

        if (rd32(d, QP_REG_CSTS) & QP_CSTS_CFS) {         /* fatal status: reset now */
            atomic_fetch_add(&d->st_cfs, 1);
            do_reset(d);
            continue;
        }
        uint64_t now = now_ns();
        if (now - last_scan >= 1000000ull) {               /* timeout scan, <=1 kHz */
            last_scan = now;
            if (any_expired(d, now)) {
                reap_all(d);                                /* a late CQE may have just landed */
                if (any_expired(d, now_ns())) do_reset(d);
            }
        }
    }
    return NULL;
}

/* ---- open / close ---------------------------------------------------------- */
static void queue_destroy(qp_driver *d, queue_t *q)
{
    if (q->sq_mem.va) qp_dma_free(d, &q->sq_mem);
    if (q->cq_mem.va) qp_dma_free(d, &q->cq_mem);
    free(q->slots);
    pthread_mutex_destroy(&q->lock);
    pthread_cond_destroy(&q->slot_cv);
}

int qp_open(qp_driver **out, const qp_hw *hw, const qp_config *in)
{
    qp_driver *d = calloc(1, sizeof *d);
    if (!d) return -ENOMEM;
    d->hw = hw;
    if (in) d->cfg = *in;
    if (!d->cfg.nqueues)      d->cfg.nqueues = 1;
    if (!d->cfg.depth)        d->cfg.depth = 64;
    if (!d->cfg.timeout_ms)   d->cfg.timeout_ms = 1000;
    if (!d->cfg.max_retries)  d->cfg.max_retries = 4;
    if (!d->cfg.hw_timeout_ms) d->cfg.hw_timeout_ms = 1000;
    d->nq = d->cfg.nqueues;
    d->epfd = d->stop_fd = -1;
    pthread_mutex_init(&d->dma_lock, NULL);
    atomic_init(&d->dead, false);
    atomic_init(&d->stopping, false);

    int rc = -EINVAL;
    uint64_t cap = qp_ld64(bar(d) + QP_REG_CAP);
    unsigned depth = d->cfg.depth;
    if (d->nq > hw->max_queues || d->nq > (unsigned)((cap >> 16) & 0xFF)) goto fail_early;
    if (depth < 2 || depth > (cap & 0xFFFF) || (depth & (depth - 1))) goto fail_early;
    if (rd32(d, QP_REG_BLKSZ) != QP_BLOCK_SIZE) goto fail_early;

    d->npages = (hw->bar_size - QP_ARENA_OFF) / QP_PAGE;
    d->page_used = calloc(d->npages, 1);
    if (!d->page_used) { rc = -ENOMEM; goto fail_early; }

    for (unsigned i = 0; i < d->nq; i++) {
        queue_t *q = &d->q[i];
        pthread_mutex_init(&q->lock, NULL);
        pthread_cond_init(&q->slot_cv, NULL);
        q->id = i; q->depth = depth; q->mask = depth - 1; q->nslots = depth - 1;
        q->sq_db = bar(d) + QP_DB_BASE + i * QP_DB_STRIDE + QP_DB_SQ_TAIL;
        q->cq_db = bar(d) + QP_DB_BASE + i * QP_DB_STRIDE + QP_DB_CQ_HEAD;
        q->slots = calloc(q->nslots, sizeof(slot_t));
        if (!q->slots || qp_dma_alloc(d, (size_t)depth * sizeof(qp_sqe), &q->sq_mem) ||
            qp_dma_alloc(d, (size_t)depth * sizeof(qp_cqe), &q->cq_mem)) {
            d->nq = i + 1;
            rc = -ENOMEM;
            goto fail;
        }
        q->sq = q->sq_mem.va;
        q->cq = q->cq_mem.va;
        for (unsigned j = 0; j < q->nslots; j++) q->slots[j].next_free = (int)j + 1;
        q->slots[q->nslots - 1].next_free = -1;
        q->free_head = 0;
        queue_reset_state(q);
        d->total_slots += q->nslots;
    }

    if (hw_disable(d) || hw_enable(d)) { rc = -EIO; goto fail; }

    d->epfd = epoll_create1(EPOLL_CLOEXEC);
    d->stop_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (d->epfd < 0 || d->stop_fd < 0) { rc = -errno; goto fail; }
    struct epoll_event ev = { .events = EPOLLIN };
    ev.data.fd = d->stop_fd;
    epoll_ctl(d->epfd, EPOLL_CTL_ADD, d->stop_fd, &ev);
    for (unsigned i = 0; i < d->nq; i++) {
        ev.data.fd = hw->irq_fd[i];
        epoll_ctl(d->epfd, EPOLL_CTL_ADD, hw->irq_fd[i], &ev);
    }
    if (pthread_create(&d->reaper, NULL, reaper_main, d)) { rc = -EAGAIN; goto fail; }
    d->reaper_started = true;
    *out = d;
    return 0;

fail:
    for (unsigned i = 0; i < d->nq; i++) queue_destroy(d, &d->q[i]);
    if (d->epfd >= 0) close(d->epfd);
    if (d->stop_fd >= 0) close(d->stop_fd);
    free(d->page_used);
fail_early:
    pthread_mutex_destroy(&d->dma_lock);
    free(d);
    return rc;
}

void qp_close(qp_driver *d)
{
    if (!d) return;
    atomic_store(&d->stopping, true);
    uint64_t one = 1;
    ssize_t r = write(d->stop_fd, &one, sizeof one);
    (void)r;
    if (d->reaper_started) pthread_join(d->reaper, NULL);

    hw_disable(d);                                   /* device stops touching our memory */

    fin_t *fins = malloc((d->total_slots ? d->total_slots : 1) * sizeof *fins);
    size_t nfin = 0;
    for (unsigned i = 0; i < d->nq; i++) pthread_mutex_lock(&d->q[i].lock);
    if (fins) nfin = drain_inflight_locked(d, fins, -ECANCELED);
    for (unsigned i = 0; i < d->nq; i++) {
        pthread_cond_broadcast(&d->q[i].slot_cv);
        pthread_mutex_unlock(&d->q[i].lock);
    }
    for (size_t i = 0; i < nfin; i++) {
        atomic_fetch_add(&d->st_completed, 1);
        fins[i].cb(fins[i].ctx, fins[i].status, fins[i].result);
    }
    free(fins);

    for (unsigned i = 0; i < d->nq; i++) queue_destroy(d, &d->q[i]);
    close(d->epfd);
    close(d->stop_fd);
    free(d->page_used);
    pthread_mutex_destroy(&d->dma_lock);
    free(d);
}

void qp_stats_get(qp_driver *d, qp_stats *o)
{
    o->submitted = atomic_load(&d->st_submitted);
    o->completed = atomic_load(&d->st_completed);
    o->retried   = atomic_load(&d->st_retried);
    o->timeouts  = atomic_load(&d->st_timeouts);
    o->resets    = atomic_load(&d->st_resets);
    o->cfs_events = atomic_load(&d->st_cfs);
    o->spurious  = atomic_load(&d->st_spurious);
    o->max_inflight = 0;
    for (unsigned i = 0; i < d->nq; i++) {
        pthread_mutex_lock(&d->q[i].lock);
        if (d->q[i].max_inflight > o->max_inflight) o->max_inflight = d->q[i].max_inflight;
        pthread_mutex_unlock(&d->q[i].lock);
    }
}
