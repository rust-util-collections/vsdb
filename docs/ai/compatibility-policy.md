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

## Typed-handle tags and wire decoders

The v18 runtime accepts only the current universe, engine, registry, raw, and typed envelopes (`VSUNI018`, `VSENG018`, `VSNREG02`, `VSMAPX02`, `VSTYPE04`). Put all historical decoders (`VSMAPX01`, `VSTYPE02`, `VSTYPE03`, v16/v17 layouts), encoders for migration, and resumable format converters in the independent `vsdb_migrate` crate (`patterns/migration.md`). Verify migration with frozen source fixtures, interrupted-retry tests, and the normal v18 reader.

- **Nominal type tags:** `VSTYPE04` hashes the fully qualified Rust type name (including generic arguments) with FNV-1a. Moving a persisted type across modules or changing a struct/enum to a type alias changes `std::any::type_name` and requires explicit translation in `vsdb_migrate` (`legacy_tag` and mixed-type child handles). Application schema changes under a stable name require application-owned versioning.
- **Strict EOF decoding:** Decode metadata files, frames, canonical keys, and values with `postcard::take_from_bytes` and reject non-empty trailing slices (`postcard::from_bytes` silently ignores trailing bytes and can truncate evolved structs on `ValueGuard` write-back).

## Accepted breaks

Only when necessary or compatibility cost is disproportionate — never as an
undocumented patch surprise. An agent must not decide the break is accepted.
Until the user explicitly accepts it in this conversation, do not ship it and do
not major-bump; leave it Open.

1. Major bump all three workspace crates (`vsdb_core`, `vsdb`, `vsdb_migrate`) in lockstep; update root `vsdb_core` and `migration` `vsdb` deps.
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
