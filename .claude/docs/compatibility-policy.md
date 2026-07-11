# Compatibility and Migration Policy

VSDB persists handles, metadata, keys, values, graph/tree nodes, namespace
registry state, and format markers. Compatibility is a correctness property.

## Default

Preserve public API behavior and existing on-disk data by default. Persisted
tags, magic bytes, key layouts, enum discriminants, and metadata envelopes are
wire protocol:

- existing tags/meanings are frozen;
- new variants/fields use append-only versioning or explicit new tags;
- never rely on source enum order for persisted discriminants;
- add old-fixture decode and round-trip tests for format changes.

## Accepted breaking changes

A break is allowed when it is genuinely necessary or compatibility cost is
disproportionate. It must not ship as an undocumented patch-level surprise.

1. Bump both crates to the next major version in lockstep and update the
   workspace `vsdb_core` dependency.
2. Document the exact broken API or persisted format and affected old versions
   in `CHANGELOG.md` plus the relevant public/migration documentation.
3. State what happens when the new version sees old data: reject loudly,
   migrate in place, or require export/reimport.
4. Provide a concrete migration procedure, including backup/rollback guidance
   and any required old-version export step.
5. Add tests proving both the intended rejection/migration and new-format
   stability.

If no safe automated migration exists, say so explicitly and prescribe full
read/export with the old version followed by import into a fresh new-version
namespace/base directory.

## Review questions

- Can an old handle/meta/data directory be opened safely?
- Can an old binary misread new data instead of rejecting it?
- Are public behavior changes observable to existing callers?
- Are all format/tag constants and migration docs updated together?
