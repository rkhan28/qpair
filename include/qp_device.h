/*
 * qp_device.h - emulated storage controller.
 *
 * Runs as its own thread and behaves like hardware: it only ever communicates
 * through the BAR (registers + doorbells + the DMA arena), a "kick" eventfd
 * standing in for the doorbell write reaching the chip, and one eventfd per
 * queue standing in for an MSI-X vector. Completions are posted out of order
 * after a randomized latency, and a set of fault injectors lets tests break it
 * in the ways real silicon breaks.
 */
#ifndef QP_DEVICE_H
#define QP_DEVICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "qp_regs.h"

typedef struct qp_device qp_device;

typedef struct {
    unsigned max_queues;       /* <= QP_MAX_QUEUES, 0 -> 8 */
    uint64_t capacity_blocks;  /* 0 -> 65536 (32 MiB) */
    size_t   arena_bytes;      /* DMA arena size, 0 -> 16 MiB */
    unsigned lat_min_us;       /* completion latency range */
    unsigned lat_max_us;
    uint64_t seed;             /* 0 -> fixed default (reproducible) */
} qp_dev_config;

/* What the driver is handed: the moral equivalent of a mapped PCI BAR. */
typedef struct {
    void    *bar;
    size_t   bar_size;
    int      kick_fd;                  /* doorbell write notification */
    int      irq_fd[QP_MAX_QUEUES];    /* per-queue interrupt */
    unsigned max_queues;
} qp_hw;

typedef struct {
    uint64_t fetched, completed, errors_injected, dropped, spurious, resets;
} qp_dev_stats;

qp_device    *qp_dev_create(const qp_dev_config *cfg);
void          qp_dev_destroy(qp_device *d);
const qp_hw  *qp_dev_hw(qp_device *d);
void          qp_dev_stats_get(qp_device *d, qp_dev_stats *out);

/* Fault injection. All thread-safe; effects apply on the device thread. */
void qp_dev_set_latency(qp_device *d, unsigned min_us, unsigned max_us);
void qp_dev_set_error_ppm(qp_device *d, uint32_t ppm);  /* transient error status */
void qp_dev_set_drop_ppm(qp_device *d, uint32_t ppm);   /* swallow the completion */
void qp_dev_drop_next(qp_device *d, unsigned n);        /* swallow the next n completions */
void qp_dev_hang(qp_device *d, bool on);                /* stop all processing (until reset) */
void qp_dev_fatal(qp_device *d);                        /* latch CSTS.CFS (until reset) */
void qp_dev_set_dead(qp_device *d, bool on);            /* reset no longer recovers it */
void qp_dev_inject_spurious(qp_device *d, unsigned qid, uint16_t cid);

#endif
