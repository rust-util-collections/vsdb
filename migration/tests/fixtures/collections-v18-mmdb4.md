# Physical v18 application fixture

`collections-v18-mmdb4.postcard` contains the 19 regular files of a real
VSDB v18 universe written by revision `6ddaed9`, using MMDB 4.3.15. Its
wire type is `Vec<(String, Vec<u8>)>` sorted by relative filename. The
fixture includes a derived namespace, v4 MANIFEST, WAL and SST, universe
and engine headers, allocator ceiling, saved collection metadata and
application-owned `__CUSTOM__/oracle`. The oracle contains a typed map
handle, a commit and its expected Merkle root, DAG handles and edge token,
SlotDex and VecDex identities, and ordered nearest-neighbor distance bits.

To generate a fresh equivalent fixture, copy [the generator](collections-v18-mmdb4.rs)
to `strata/examples/cutover_fixture.rs` in a disposable checkout at
`6ddaed9`. Run `cargo +stable run -p vsdb --example cutover_fixture -- /absolute/path/to/new-base`
with an isolated `VSDB_BASE_DIR`. After the process exits, serialize the
sorted list of relative filenames and unmodified file bytes with postcard.
Randomly generated universe IDs, graph levels, filenames and timestamps
belong to each generated store. The test reads the expectations captured
by its own writer. [End-to-end test](../collections_generation.rs)
