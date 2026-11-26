# Pragmatic Engineering

Skills and reviews serve correct, durable VSDB behavior. Ceremony, metrics, and
process artifacts are not goals.

## Maintenance scope

The four project skills review and maintain this local Rust database workspace.
Ground each task in a named collection, storage operation, or compatibility
contract. Explain the expected result, the observed result, and the smallest
change that restores the contract. Full-repository scope broadens file coverage;
it does not change the kind of work being requested.

Use precise engineering terms: Rust `unsafe` concerns memory and aliasing
contracts; Merkle proof verification concerns key/root/hash consistency;
crash recovery concerns interrupted writes and reopening local storage. Preserve
these technical meanings instead of substituting vague or dramatic labels.

Prefer existing tests and small regression cases with temporary data. Use
test doubles or isolated child-process fixtures when a storage-failure or
restart sequence is needed. Report tool/access failures accurately and continue
independent work that remains possible; wording changes do not establish that a
previously blocked operation is permitted or has succeeded.

## Performance evidence

Choose the control that answers the claim. An upgrade comparison runs the
same VSDB operation on v4-backed and v5-backed v18. An adapter optimization
compares two implementations on the same MMDB v5 backend. Keep the data,
durability, offered load and cache regime matched. Compare paired results
with control-to-control variation and record the verified binary revisions.
Use the ordinary build for latency and treat diagnostic counters separately.

At saturation, report small-put and bulk completion rates separately from
aggregate bytes/s. For paced writes, include arrival-to-ack p99, unserved
work and end-of-window storage debt. For queries, check ordered results and
time the whole API call. Use actual I/O counters for device-read claims.
[Adapter trials](../evaluations/v18-adapter/README.md) · [Publication trials](../evaluations/v18-publication/README.md) · [VecDex query controls](../evaluations/v18-vecdex-v5/README.md)

## Engineering choices

Simplicity = stable, deterministic execution — not shorter text.

- **Keep signal**: explicit names, file:line evidence, safety steps that remove guesswork.
- **Cut variance**: duplicate truth, discretionary branches, rework without new evidence, speculative features, risk-free ceremony.
- **Keep controls**: one-issue-one-commit, worktree ownership, independent evidence checks, interrupted-write regression tests, compatibility gates, final workspace gates.
- **Admit only improvements that name the ambiguity or failure they remove.**
