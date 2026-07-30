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

## Accepted breaks

Only when necessary or compatibility cost is disproportionate — never as an
undocumented patch surprise.

1. Major bump both crates lockstep; update workspace `vsdb_core` dep.
2. Document broken API/format and affected old versions in `CHANGELOG.md` + public migration docs.
3. State old-data behavior: hard reject, in-place migrate, or export/reimport.
4. Concrete migration (backup/rollback; any old-version export step).
5. Tests for rejection/migration and new-format stability.

If no safe auto-migration: say so; require old-version full export then import into a fresh new-version root.

## Review questions

- Old handle/meta/dir opens safely?
- Old binary misreads new data instead of rejecting?
- Public behavior change observable to callers?
- All tag constants + migration docs updated together?
