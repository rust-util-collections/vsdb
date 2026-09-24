# Compatibility and Migration Policy

VSDB persists handles, metadata, keys/values, graph/tree nodes, namespace
registry state, and format markers. Compatibility is correctness.

## Default

Preserve public API and existing on-disk data. Persisted tags, magic, key
layouts, enum discriminants, and metadata envelopes are wire:

- existing tag meanings frozen;
- new fields/variants: append-only or explicit new tags;
- never trust source enum order for on-disk discriminants;
- format changes need old-fixture decode + round-trip tests.

## Typed-handle tags

Typed handles serialize as magic + 8-byte type tag + payload.

- `VSTYPE03` (v17+) tags the **fully qualified** Rust type name, including
  every generic parameter. Same-named types in different modules must not
  share a tag. Moving or renaming a persisted type needs an explicit migration;
  type aliases that still refer to the original type retain its identity.
- Tags check nominal identity, not schema: field/layout changes under the
  same name still require application schema versioning and conversion.
- `VSTYPE02` (v16) metas are still restored, checked against the **full**
  `type_name` — so wrapper types and every type used inside built-in handles'
  parameters keep their paths while v16 data may exist.
  The library's transparent `BranchId` / `CommitId` newtypes use their v16
  `u64` alias names for this legacy check, including nested parameters;
  `VSTYPE03` continues to distinguish both ids from each other and from `u64`.
- A new envelope needs a new magic; keep reading the old ones.

The short-name `VSTYPE03` experiment was removed before v17 publication
(owner-authorized pre-release redesign, 2026-09-24). Its development-only
metadata is not a supported format; do not add a permissive fallback that
reintroduces wrong-type restoration.

## Accepted breaks

Only when necessary or compatibility cost is disproportionate — never as an
undocumented patch surprise. An agent must not decide the break is accepted.
Until the user explicitly accepts it in this conversation, do not ship it and do
not major-bump; leave it Open.

1. Major bump both crates lockstep; update workspace `vsdb_core` dep.
2. Document broken API/format and affected old versions: public migration docs in
   the behavior commit; `CHANGELOG.md` in the release commit (`commit-protocol.md`).
3. State old-data behavior: hard reject, in-place migrate, or export/reimport.
4. Concrete migration (backup/rollback; any old-version export step).
5. Tests for rejection/migration and new-format stability.

If no safe auto-migration: say so; require old-version full export then import into a fresh new-version root.

## Review questions

- Old handle/meta/dir opens safely?
- Old binary misreads new data instead of rejecting?
- Public behavior change observable to callers?
- All tag constants + migration docs updated together?
