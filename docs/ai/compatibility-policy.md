# Compatibility and Migration Policy

VSDB persists handles, metadata, keys/values, graph/tree nodes, namespace
registry state, and format markers. Compatibility is correctness.

## Default

Preserve public API and existing on-disk data. Persisted tags, magic, key
layouts, enum discriminants, and metadata envelopes are wire:

- existing tag meanings frozen;
- new fields/variants: append-only or explicit new tags;
- never trust source enum order for on-disk discriminants;
- format changes need old-fixture decode + round-trip tests. A `#[serde(default)]` field alone does not establish postcard compatibility.

## Supported architectures

VSDB guarantees correct behavior on `x86_64` and `aarch64`. Other architectures
carry no testing or correctness guarantee. This support boundary applies to
runtime behavior, review and release validation.

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

## Shard and collection formats

Check the physical engine format and the logical VSDB format independently.
VSDB v19 opens v18 universes in MMDB format 5. Verify namespace placement, saved
handles, collection contents and writable reopen across the upgrade.
[Upgrade an existing universe](../upgrade.md)

## Typed-handle tags and wire decoders

The runtime checks the universe, engine, registry, raw and typed envelopes
(`VSUNI018`, `VSENG018`, `VSNREG02`, `VSMAPX02`, `VSTYPE04`). Keep those
identities and the MPT and SMT root and proof encodings stable. Exercise root
hashes, proof verification and serialization when changing hash or transport code.
[Native v18 proof fixture](../../strata/tests/fixtures/v18-roots-proofs.postcard)

- **Nominal type tags:** `VSTYPE04` hashes the fully qualified Rust type name
  (including generic arguments) with FNV-1a. Moving or renaming a persisted
  type can change its tag. Applications version their value schemas under
  a stable type name.
- **Strict EOF decoding:** Decode metadata files, frames, canonical keys, and values with `postcard::take_from_bytes` and reject non-empty trailing slices (`postcard::from_bytes` silently ignores trailing bytes and can truncate evolved structs on `ValueGuard` write-back).

## Accepted breaks

Only when necessary or compatibility cost is disproportionate — never as an
undocumented patch surprise. An agent must not decide the break is accepted.
Until the user explicitly accepts it in this conversation, do not ship it and do
not major-bump; leave it Open.

1. Bump both workspace crates (`vsdb_core`, `vsdb`) in lockstep and update the root `vsdb_core` dependency.
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
