# Design notes

Why the code looks the way it does, and how it was verified.

## Memory ordering is the protocol

Host and device share rings in memory and communicate through two kinds of publish operation. Both use the same pattern: write the payload with plain stores, then publish it with a single release store that the other side reads with acquire.

| Direction | Payload | Publish | Consume |
|-----------|---------|---------|---------|
| host to device | 64-byte SQE | release-store SQ tail doorbell | device acquire-loads the tail, then copies the SQE |
| device to host | CQE body | release-store `dw3` (cid, phase, status) | host acquire-loads `dw3`, checks phase, then reads the body |

The phase bit is what makes this lock-free: the host never needs to read a device-written head pointer to know whether a CQE is new; it compares the entry's phase to the phase it expects, and the expectation flips each time the ring wraps.

## Capacity rule: depth - 1 slots

`head == tail` has to mean "empty", so a ring of depth `N` holds at most `N-1` entries. The driver ties its command slots to that limit, so the free-slot list is the only backpressure mechanism needed and the SQ cannot overflow. The device still checks for a full CQ before posting rather than trusting the host.

## Command IDs carry a generation

`cid = (generation << 12) | slot`. The generation increments each time a slot is reused. A completion that names an idle slot, an out-of-range slot, or a busy slot with the wrong generation is counted as spurious and ignored. Without this, a duplicate or late completion for an old command could complete whatever request currently occupies that slot.

## Recovery model

The device is assumed to be able to lose work silently, so recovery cannot depend on the device cooperating.

- **Triggers:** an expired per-command deadline, or `CSTS.CFS` (fatal status). CFS is polled by the reaper, so a fatal device is recovered long before a long command timeout would fire.
- **Reset sequence:** take every queue lock, `CC.EN=0`, wait for `RDY=0` (the device has stopped touching our memory), zero rings and reset head/tail/phase, re-enable, wait for `RDY=1`, then re-post every in-flight command.
- **Idempotence:** a lost completion can mean the work already happened. The device deliberately drops completions *after* executing, so the retry path re-executes writes. This is safe because read, write and flush are all idempotent.
- **Fairness:** only commands whose own deadline expired are charged a retry. One lost completion resets the whole device, and charging bystanders would let one bad command burn through unrelated I/O's retry budget.
- **Giving up:** if the device will not return to `RDY` within `hw_timeout_ms`, the driver marks it dead, fails every in-flight command with `-EIO`, and rejects new submissions with `-ENODEV`.

## Retry in place for transient errors

A transient error status re-posts the command under the same cid. The device has already completed the previous attempt, so nothing else is using that cid, and keeping it leaves the slot bookkeeping untouched.

## Locking

One mutex per queue covers the SQ tail, free-slot list and slot table. Submitters hold it only to claim a slot and write one SQE. The reaper takes it per completion and always releases it before running the user callback. A reset holds all queue locks, which is what keeps submitters out while rings are being torn down. Callbacks run on the reaper thread and must not block.

## Verification

- **Unit and fault tests:** 15 tests (`make test`), each failure mode the driver claims to handle has its own test.
- **ASan, UBSan, LeakSanitizer:** clean, including the shutdown-with-I/O-in-flight test.
- **ThreadSanitizer:** clean. To check that this result means something, the release/acquire pairs in `qp_regs.h` were temporarily weakened to relaxed. TSan immediately reported data races on the CQE body reads, the device-side DMA copy and the test data buffers, so the ordering in the real code is doing real work and TSan can see it.
- **A test that was wrong, not the driver:** an early version of the forged-completion test injected 18 CQEs into a depth-16 ring. The device correctly refused to overflow the CQ and some injections never arrived. The test now uses a ring large enough to hold them.

## Known limitations

- One reaper thread serialises all callbacks. Sharding per queue would be the next step and needs the reset path to coordinate across reapers.
- The device shares an address space with the driver. This lets TSan check the protocol but is less realistic than a separate process.
- No admin queue, namespaces, scatter-gather lists or interrupt coalescing.
