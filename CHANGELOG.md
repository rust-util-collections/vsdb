# CHANGE LOG

## v4.x.x

- Migrate to the 2024 edition of Rust
- Bump `parity-db` and other dependencies
- Fix the `total` issue in `slot_db`
- Rename generic types for clarity
- Optimize variable naming
- Declare `len` as a hint in `slot_db`

## v3.x.x

- Make `<Type>::new()` lazyable
  - 'Copy On Write' style
- Set `paritydb` as the default backend to save compilation time

## v1.x.x

- Simplify functions and release v1.0.0

## v0.70.x

- Backport the changes of the `mmdb` crate.

## v0.61.x

#### Internal optimizations

- Add a new optional engine based on `parity-db`
  - Enable the `rocks_backend` feature to use `rocksdb`, default
  - Enable the `parity_backend` feature to use the `parity-db`

## v0.60.x

#### Internal optimizations

- Optimize performance

## v0.59.x

#### Internal optimizations

- Remove all `sled` related features

#### BUG fixes

- Fix `range` on negative integers(key)

## v0.57.x

#### Internal optimizations

- Switch the default engine to `rocksdb` globally
- Switch the default codec from `bcs` to `msgpack`
- Add a new sub crate: [**slot db**](utils/slot_db)

## v0.56.x

#### Internal optimizations

- Tuning details
- Bump the version of `rocksdb-rs`

#### New APIs

- Orphan: add two new APIs
  - `is_uninitialized`
  - `initialize_if_empty`
- Orphan: make `set_value` public

## v0.55.x

#### Function changes

- The `sled` project looks dead, so we switch the default backend to `rocksdb`

## v0.54.x

#### Function changes

- Disable the embed serde-based implementations of `KeyEnDe` and `ValueEnDe` by default

## v0.53.x

#### New APIs

- Add various trie/mpt based functions

#### Function changes

- Disable the "vs" feature by default

## v0.52.x

#### Function changes

- Extend the feature of "derive" to "vs"

## v0.51.x

#### Function changes

- Use `bcs` as the default codec

#### Internal optimizations

- Make documents consensus with codebase

## v0.50.x

#### Internal optimizations

- Implement KeyEnDeOrdered for some `primitive_types`
- Optimize the designs of some encoding related traits
  - `KeyEn`, `KeyDe`, `KeyEnDe`
  - `ValueEn`, `ValueDe`, `ValueEnDe`

## v0.49.x

#### BUG fixes

#### Internal optimizations

- Switch the default codec from `json` to `bcs`
- Add 'message pack' back to the list of alternative codec
  - coresponding feature: `msgpack`

## v0.48.x

#### BUG fixes

- Fix issues related to the `prune` operation

#### Internal optimizations

- Tuning feature settings and code details
- Add embed supports for 'primitive-types v0.12.x'
- Switch the default codec from `msgpack` to `json`
  - For better generality and compatibility

## v0.47.x

#### New APIs

- Add more conversion functions for Version/Branch related structures

#### BUG fixes

- Fix issues related to the `prune` operation

## v0.46.x

No functional changes, just upgraded some necessary dependencies.

## v0.45.x

#### Internal optimizations

- Avoid storing fields that can be derived from other fields
  - Save space
  - Improve efficiency
  - **BUT** the memory usage during runtime will increase
  - **BUT** the time for the process to restart will increase
- Enhance atomicity guarantees during data changes

#### API changes

- Use shorter names in all APIs
  - `branch_id` ==> `br_id`
  - `branch_name` ==> `br_name`
  - `version_id` ==> `ver_id`
  - `version_name` ==> `ver_name`

## v0.44.x

#### Internal optimizations

- Those 'deleted' KVs of the oldest version will be cleaned up
- Enhance the ability of `prune` and optimize its performance
- Start a background thread to clean up orphaned instances asynchronously
- Enable the embed lru-cache of rocksdb if `feature = "rocks_engine"`
- Make the supports for the `primitive-types` crate optional

#### New APIs

- `version_chgset_trie_root`
  - Return the 'trie' root hash of the change set of the target version
  - NOTE: this is a realtime-compution operation

## v0.43.x

#### Internal optimizations

- Optimize performance
  - Tuning low-level en/decoding mechanism
  - Avoid `iter().last()`, use `iter().next_back()`

## v0.42.x

#### API changes

- Split the original all-in-one crate into two pieces
  - `vsdb`, path: "vsdb/wrappers"
  - `vsdb_core`, path: "vsdb/core"
- The semantic of `clone` has been changed to a deep copy
  - The semantic of the previous `clone` is inherited by `shadow`
  - NOTE: the newly added `shadow` API is marked as `unsafe`
- Add `_mut` methods for various `Iter`s
  - `iter_mut`
  - `values_mut`
  - `range_mut`
- The low-level "does not exist" expression has been changed from 'None' to `&[]`
  - If you assign a `&[]`, `vec![]` or `Box<[]>` value to a key, the key will be treated as 'deleted'

#### Internal optimizations

- Remove lru cache
- Tuning `area idx` for LSM-friendly storage
- Optimize the implementation of inner length counter

## v0.41.1

#### Internal optimizations

- Enable lru cache when using sled as backend engine

## v0.41.0

#### API changes

- `write` functions now have `&mut self` definations

## v0.37.2

#### Metainfo changes

- Migrate the repo address to 'github.com/rust-util-collections'

## v0.36.0

#### Internal optimizations

- Optimize conditional compilation attributes
- Enable the `compress` feature by default

## v0.35.3

#### Internal optimizations

- Use absolute importing path in the scope of macro definations

## v0.35.0

#### Function changes

- The embed MerkleTree is removed

## v0.34.3

#### Internal optimizations

- Relax the K/V binding conditions of all structured kinds

## v0.34.2

#### Unit tests

- Add more unit tests, test coverage reached 75%

## v0.34.1

#### Internal optimizations

- Tuning options of the backend DB

## v0.34.0

#### New APIs

- `branch_keep_only`: delete all other branches out of the target list
  - Will also clean up all orphan versions

#### BUG fixes

- Instance length counters are not atomic
  - Multi-thread safety cannot be guaranteed
- `branch_remove`: the cleanup of branch id is incomplete
  - The `branch_id_to_branch_name` field is dirty

## v0.33.0

#### API changes

- Change the callback parameter definition of `iter_op`

## v0.32.0

#### New APIs

Add iter-like functions in `MapxMkVs` and its derivatives.

- `iter_op`
- `iter_op_by_branch`
- `iter_op_by_branch_version`
- `iter_op_with_key_prefix`
- `iter_op_with_key_prefix_by_branch`
- `iter_op_with_key_prefix_by_branch_version`

## v0.31.4

#### BUG fixes

- Fix incorrect logic in the `branch_has_versions` function

## v0.31.3

#### BUG fixes

- Fix incorrect logic in the `prune` function

#### Internal optimizations

- Allow creating new branches from an empty base branch
- The initial version should NOT exist
  - It may make `prune` useless(common versions may never exist)
- The results of those `xxx_list_xxx` APIs are unreliable
  - Will try to return the first non-empty list for reference since this version
  - The branches or versions of every child instance of a composite Vs instance may be different
  - > **For example:**</br>there are three Vs-structures `struct Vs0(Vs1, Vs2); struct Vs1; struct Vs2`,</br>the caller of `Vs0` cannot guarantee that other callers will not directly create branches and versions on `Vs1` or `Vs2`.

## v0.30.0

#### APIs changes

- `branch_create_xxx`: add a 'force' param to all 'branch_create' prefixed functions
  - if the value of 'force' is true, a branch with the same name of the target branch will be removed automatically

## v0.29.0

#### BUG fixes

- Avoid incorrect panic in some APIs of `OrphanVs`
  - `get_value -> Value` :=> `get_value -> Option<Value>`
  - `get_mut -> MutValue` :=> `get_mut -> Option<MutValue>`

## v0.28.0

#### New APIs

- `branch_swap`: swap the underlying instance of two branches
  - **Unsafe**
    - Non-'thread safe'
    - Must ensure that there are no reads and writes to these two branches during the execution
  - Logically similar to `std::ptr::swap`
  - > **For example:**</br>If you have a master branch and a test branch, the data is always trial-run on the test branch, and then periodically merged back into the master branch.</br>Rather than merging the test branch into the master branch, and then recreating the new test branch, it is more efficient to just swap the two branches, and then recreating the new test branch.
- `branch_is_empty`: check if the specified branch is empty
  - 'empty' means that no actual data exists on this branch even if there are some empty versions on it
- `branch_get_default`: get the default branch name of the specified instance
- `branch_list`: list all branch names of the specified instance
- `version_list`: list all version names of the default branch
- `version_list_by_branch`: list all version names of the specified branch
- `version_list_globally`: list all version names of the global scope
  - NOTE: include orphaned versions
- `version_exists_globally`: check if a version exist in the global scope
  - NOTE: include orphaned versions
- `version_has_change_set`: check if a version has made some actual changes
- `version_clean_up_globally`: clean all orphraned versions in the global scope
- `version_revert_globally`: make the specified version disappear from the gloal scope
  - **Unsafe**
  - Version itself and its corresponding changes will be completely purged from all branches

## v0.27.3

#### Internal optimizations

- For orphan versions, no real-time cleanup will be done
  - They will be cleaned up at the time of a 'prune' operation
  - Call `version_clean_up_globally` manually when you need it

## v0.27.2

#### Internal optimizations

- Ignore empty branches during pruning

## v0.27.0

#### New APIs

- `branch_merge_to_force`: merge your branch into the target branch even if different new versions exist on the target branch
  - **Unsafe**

#### APIs changes

- `prune`: no longer executed on the default branch, the logic now is to calculate the common prefix of the version list of all non-empty branches, and then prune this prefix list
- `prune_by_branch`: **was removed**
- `branch_merge_to_parent` :=> `branch_merge_to`: support for safely merging into any static ancestor branch

#### External function changes

- The version merge operation is no longer limited to merging into its parent branch
  - Support merging into any existing branch
- The original branch that was merged will no longer be automatically deleted
- Rename the ambiguous 'Rk' suffix in some structures to 'RawKey'
- Rename the ambiguous 'Rv' suffix in some structures to 'RawValue'

#### Internal optimizations

- Each branch will keep a complete copy of the version sequence
  - Make data indexing more efficient
  - Eliminate the need for recursive queries
- The branch level is removed from the key-value multiple map
  - The version id is guaranteed to be unique in the global scope
  - So this level has no practical significance any more
