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

## Rust toolchain

Use the official latest stable Rust for edits, dependency updates, formatting,
Clippy, tests, CI and releases. Run `rustup update stable` before Rust validation
and select it with `RUSTUP_TOOLCHAIN=stable` or `cargo +stable`; an installed
channel named `stable` may need updating.

The repository defines no compatible compiler range or minimum Rust version.
Do not add `rust-version`, numeric toolchain pins, minimum-version CI jobs or
compatibility shims for older compilers.

Historical CHANGELOG and evaluation baselines retain the toolchain notes
recorded at the time. This toolchain policy does not authorize public API or
persisted-format breaks or automatic major bumps.

## Typed-handle tags

The v18 runtime accepts the current raw and typed envelopes, `VSMAPX02` and `VSTYPE04`. Put all historical decoders, encoders for migration, and format converters in the independent `vsdb_migrate` crate. Verify migration with frozen source data and the normal v18 reader.

The type tag is FNV-1a over the fully qualified Rust type name, including generic parameters. Nominal identity can change with a module move. Application schema changes under a stable name also require application-owned versioning.

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
