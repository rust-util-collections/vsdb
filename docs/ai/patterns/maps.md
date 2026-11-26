# Map keys, aliases and value guards

## Aliases and write-back

- Check the write exclusion required by `shadow()`. Disjoint keys may be
  written through aliases of a plain map. Writes to the same key and
  structural edits spanning multiple keys require the documented exclusion.
- Both occupied and vacant `Entry` variants retain an exclusive map borrow.
  A `ValueGuard` writes back a vacant insertion or a value accessed through
  `DerefMut`. Debug builds diagnose interior mutations through a shared
  dereference.
- Trace `UnwindMark::interrupted()` when a guard drops. An unwind that
  started while the guard was live discards interrupted edits. A guard
  created inside an already-unwinding destructor retains its normal
  write-back behavior. [Guard implementation](../../../strata/src/common/guard.rs)

## Key and value encoding

- Check that postcard key bytes are canonical. The blanket key bound requires
  `Eq + Hash`, which excludes floats and unordered maps and sets. Ordered
  keys use `KeyEnDeOrdered` with explicit integer widths. The ordered
  integer encodings exclude `usize` and `isize`.
- Check borrowed lookups against their owning `KeyLookup<Q>` or
  `OrderedKeyLookup<Q>` implementation. Postcard arrays omit the length
  prefix encoded for byte vectors. A wrong-length slice must miss an exact
  fixed-array key on both reads and writes.
- Exercise `postcard_key` when the fixed stack buffer fills.
  `postcard::to_slice` can return `CollectStrError` from `collect_str`
  as well as `SerializeBufferFull`. Both take the heap fallback.
- Typed equality compares decoded values, including values with more than
  one byte representation. For hand-coded nodes, check field order in both
  directions and exercise round trips. Use frozen fixtures for changes to
  persistent layouts. [Key codecs](../../../strata/src/common/ende.rs) · [Type tags and strict decoding](../compatibility-policy.md#typed-handle-tags-and-wire-decoders)
