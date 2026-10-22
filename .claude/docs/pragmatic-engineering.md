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

## Engineering choices

Simplicity = stable, deterministic execution — not shorter text.

- **Keep signal**: explicit names, file:line evidence, safety steps that remove guesswork.
- **Cut variance**: duplicate truth, discretionary branches, rework without new evidence, speculative features, risk-free ceremony.
- **Keep controls**: one-issue-one-commit, worktree ownership, independent evidence checks, interrupted-write regression tests, compatibility gates, final workspace gates.
- **Admit only improvements that name the ambiguity or failure they remove.**
