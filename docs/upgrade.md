# Upgrade an existing universe to VSDB v19

A universe consists of the base directory and all registered namespace roots.
For an upgrade, also keep the application records that identify collections,
such as saved `InstanceId` values and serialized handles.

| Existing data | Path to v19 |
| --- | --- |
| Native v18 collections on MMDB format 5 | Open in place using the procedure below. |
| v18 development collections on MMDB v4 shards | Stage the shards in v5 format using the v18 converter. Verify the existing handles against the converted data, then open it with v19. |
| v17 collections on MMDB v4 shards | Use the v18 converter to stage the shards **and** convert the affected collection layouts and application handles. Verify the resulting v18 universe before opening it with v19. |

Both conversion paths use the converter on the v18 maintenance branch.
[v18 offline migration guide](https://github.com/rust-util-collections/vsdb/blob/v18/migration/README.md)

## Open a native v18 universe in place

1. Stop the processes using the universe. Back up the base, every explicit
   namespace root and the matching application records from the same stopped
   dataset. Derived namespace roots are already inside the base.
2. Update the application to v19. Configure the same base directory before
   opening a collection. Explicit namespace paths in the registry must still
   resolve to their data. [Process configuration](../core/docs/api.md#configure-the-universe) · [Namespace paths](namespaces.md#directory-layout-and-recovery)
3. Restore collections with their original types and saved IDs or serialized
   handles. Check representative application queries before resuming writes.
   The universe identity, shard counts, collection prefixes and handle
   encodings carry over.

To roll back, restore all backed-up directories and
application records together. Writes accepted after the backup need to be
reconciled separately.
