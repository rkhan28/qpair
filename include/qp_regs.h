/*
 * qp_regs.h - the hardware/software contract.
 *
 * Everything the driver knows about the device is in this file: the register
 * map, the doorbell layout, and the wire format of submission/completion
 * entries. The driver never includes device internals, and the device never
 * includes driver internals. Layout and protocol follow NVMe's queue-pair
 * model (SQ tail / CQ head doorbells, phase-tagged completions).
 *
 * Ordering rules (the part that is easy to get wrong):
 *   host -> device:  plain-store SQE, then RELEASE-store SQ tail doorbell.
 *   device -> host:  plain-store CQE body, then RELEASE-store dw3 (phase bit).
 *   host reads dw3 with ACQUIRE, checks phase, only then reads the CQE body.
 */
#ifndef QP_REGS_H
#define QP_REGS_H

#include <stdint.h>

enum {
    QP_PAGE        = 4096,
    QP_BLOCK_SIZE  = 512,
    QP_MAX_QUEUES  = 16,
    QP_MAX_DEPTH   = 4096,  /* cid index is 12 bits */
    QP_MAX_XFER    = 256,   /* blocks per command */
};

/* --- register map (offsets into the BAR) -------------------------------- */
#define QP_REG_CAP       0x00u  /* u64 RO  [15:0] max depth, [23:16] max queues */
#define QP_REG_CC        0x08u  /* u32 RW  bit0 EN                              */
#define QP_REG_CSTS      0x0Cu  /* u32 RO  bit0 RDY, bit1 CFS (fatal)           */
#define QP_REG_NBLOCKS   0x10u  /* u64 RO  namespace size in blocks             */
#define QP_REG_BLKSZ     0x18u  /* u32 RO  block size in bytes                  */
#define QP_REG_QBASE     0x20u  /* per-queue register block, QP_QREG_STRIDE each */
#define QP_QREG_STRIDE   0x20u
#define QP_QREG_SQ_BASE  0x00u  /* u64 IOVA of submission ring                  */
#define QP_QREG_CQ_BASE  0x08u  /* u64 IOVA of completion ring                  */
#define QP_QREG_DEPTH    0x10u  /* u32 entries (power of two); 0 = unused       */

#define QP_CC_EN         0x1u
#define QP_CSTS_RDY      0x1u
#define QP_CSTS_CFS      0x2u

/* Doorbells live on their own cache lines: the submit path writes the SQ tail,
 * the completion path writes the CQ head, and they must not false-share. */
#define QP_DB_BASE       0x1000u
#define QP_DB_STRIDE     0x80u
#define QP_DB_SQ_TAIL    0x00u
#define QP_DB_CQ_HEAD    0x40u

/* IOVAs handed to the device are byte offsets from the BAR base. The DMA
 * arena (the only memory the device may touch) starts here. */
#define QP_ARENA_OFF     0x10000u

/* --- commands ------------------------------------------------------------ */
enum { QP_OP_FLUSH = 0, QP_OP_WRITE = 1, QP_OP_READ = 2 };

typedef struct {
    uint8_t  opc;
    uint8_t  flags;
    uint16_t cid;      /* [11:0] slot index, [15:12] generation */
    uint32_t nsid;
    uint64_t lba;
    uint32_t nblk;
    uint32_t rsvd0;
    uint64_t iova;     /* data buffer, must lie inside the DMA arena */
    uint64_t rsvd1[4];
} qp_sqe;
_Static_assert(sizeof(qp_sqe) == 64, "SQE must be 64 bytes");

/* dw3: [15:0] cid, [16] phase, [31:17] status */
typedef struct {
    uint32_t result;
    uint32_t rsvd;
    uint16_t sq_head;  /* device's SQ consume pointer when this CQE was posted */
    uint16_t sq_id;
    uint32_t dw3;
} qp_cqe;
_Static_assert(sizeof(qp_cqe) == 16, "CQE must be 16 bytes");

#define QP_CQE_CID(dw3)     ((uint16_t)((dw3) & 0xFFFFu))
#define QP_CQE_PHASE(dw3)   (((dw3) >> 16) & 1u)
#define QP_CQE_STATUS(dw3)  ((dw3) >> 17)
#define QP_CQE_DW3(cid, ph, st) \
    ((uint32_t)(cid) | ((uint32_t)(ph) << 16) | ((uint32_t)(st) << 17))

/* status codes */
enum {
    QP_SC_OK            = 0,
    QP_SC_INVALID_OPC   = 1,
    QP_SC_INVALID_FIELD = 2,
    QP_SC_LBA_RANGE     = 3,
    QP_SC_DMA_ERROR     = 4,
    QP_SC_INTERNAL      = 0x10,  /* transient: driver may retry */
};

/* --- shared-memory access helpers (same on both sides) ------------------- */
static inline uint32_t qp_ld32(const void *p) { return __atomic_load_n((const uint32_t *)p, __ATOMIC_ACQUIRE); }
static inline void     qp_st32(void *p, uint32_t v) { __atomic_store_n((uint32_t *)p, v, __ATOMIC_RELEASE); }
static inline uint64_t qp_ld64(const void *p) { return __atomic_load_n((const uint64_t *)p, __ATOMIC_ACQUIRE); }
static inline void     qp_st64(void *p, uint64_t v) { __atomic_store_n((uint64_t *)p, v, __ATOMIC_RELEASE); }

#endif
