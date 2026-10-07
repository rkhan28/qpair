/*
 * qp_driver.h - host-side driver for the queue-pair controller.
 *
 * Thread model: any number of application threads call qp_submit/qp_exec; one
 * internal "reaper" thread plays the interrupt handler (epoll on the per-queue
 * irq eventfds), drains completion queues, runs timeouts and performs
 * controller resets. Completion callbacks run on the reaper thread and must
 * not block.
 */
#ifndef QP_DRIVER_H
#define QP_DRIVER_H

#include <stddef.h>
#include <stdint.h>
#include "qp_device.h"

typedef struct qp_driver qp_driver;

typedef struct {
    unsigned nqueues;      /* 0 -> 1 */
    unsigned depth;        /* power of two, 0 -> 64. usable slots = depth-1 */
    unsigned timeout_ms;   /* per-command timeout, 0 -> 1000 */
    unsigned max_retries;  /* transient-error / timeout retries, 0 -> 4 */
    unsigned hw_timeout_ms;/* wait for RDY transitions, 0 -> 1000 */
} qp_config;

typedef struct {
    void    *va;
    uint64_t iova;
    size_t   len;
} qp_dma;

typedef struct {
    uint8_t  opc;          /* QP_OP_* */
    uint64_t lba;
    uint32_t nblk;
    uint64_t iova;         /* from qp_dma.iova */
} qp_cmd;

typedef struct {
    uint64_t submitted, completed, retried, timeouts, resets, cfs_events,
             spurious, max_inflight;
} qp_stats;

/* status passed to callbacks: 0 or a negative errno
 *  -ENOTSUP bad opcode   -EINVAL bad field   -ERANGE lba out of range
 *  -EFAULT bad DMA address  -EIO device error  -ETIMEDOUT retries exhausted
 *  -ECANCELED driver closed */
typedef void (*qp_done_fn)(void *ctx, int status, uint32_t result);

int  qp_open(qp_driver **out, const qp_hw *hw, const qp_config *cfg);
void qp_close(qp_driver *d);   /* no concurrent submitters; in-flight -> -ECANCELED */

int  qp_dma_alloc(qp_driver *d, size_t len, qp_dma *out);
int  qp_dma_free(qp_driver *d, qp_dma *m);

/* Returns 0 if accepted (callback fires exactly once). Blocks while the queue
 * has no free slot (backpressure). Negative errno = rejected, no callback. */
int  qp_submit(qp_driver *d, unsigned qid, const qp_cmd *cmd, qp_done_fn cb, void *ctx);
int  qp_exec(qp_driver *d, unsigned qid, const qp_cmd *cmd, uint32_t *result);

void qp_stats_get(qp_driver *d, qp_stats *out);

#endif
