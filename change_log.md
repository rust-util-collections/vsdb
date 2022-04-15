# Change log

## v0.28.0

#### New APIs

- `branch_is_empty`: check if the specified branch is empty
  - 'empty' means that no actual data exists on this branch
  - Even if there are some empty versions on it
- `branch_get_default`: get the default branch name of the specified instance
- `branch_list_globally`: list all branch names of the specified instance
- `version_list`: list all version names of the default branch
- `version_list_by_branch`: list all version names of the specified branch
- `version_list_globally`: list all version names of the global scope
  - **NOTE**: include orphaned versions
- `version_has_change_set`: check if a version has made some actual changes
- `version_clean_up_globally`: clean all orphraned versions in the global scope
- **Unsafe** `version_revert_globally`: make the specified version disappear from the gloal scope
  - Version itself and its corresponding changes will be completely purged from all branches

## v0.27.3

#### Internal structure optimizations

- For orphan versions, no real-time cleanup will be done
  - They will be cleaned up at the time of a 'prune' operation
  - Call `version_clean_up_globally` manually when you need it

## v0.27.2

#### Internal structure optimizations

- Ignore empty branches during pruning

## v0.27.0

#### External function changes

- The version merge operation is no longer limited to merging into its parent branch
  - Support merging into any existing branch
- The original branch that was merged will no longer be automatically deleted
- Rename the ambiguous 'Rk' suffix in some structures to 'RawKey'
- Rename the ambiguous 'Rv' suffix in some structures to 'RawValue'

#### Internal structure optimizations

- Each branch will keep a complete copy of the version sequence
  - Make data indexing more efficient
  - Eliminate the need for recursive queries
- The branch level is removed from the key-value multiple map
  - The version id is guaranteed to be unique in the global scope
  - So this level has no practical significance any more
