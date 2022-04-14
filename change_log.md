# Change log

## v0.27.0

#### External function changes:

- The version merge operation is no longer limited to merging into its parent branch
  - Support merging into any existing branch
- The original branch that was merged will no longer be automatically deleted
- Rename the ambiguous 'Rk' suffix in some structures to 'RawKey'
- Rename the ambiguous 'Rv' suffix in some structures to 'RawValue'

#### Internal structure optimizations:

- Each branch will keep a complete copy of the version sequence
  - Make data indexing more efficient
  - Eliminate the need for recursive queries
- The branch level is removed from the key-value multiple map
  - The version id is guaranteed to be unique in the global scope
  - So this level has no practical significance any more
