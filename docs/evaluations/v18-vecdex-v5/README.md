# VecDex v18 search on MMDB v5

## Query and decision rule

The primary workload is an ordinary warm `VecDex<u64, L2>::search` requesting
five neighbors from a persisted 8,192-node HNSW graph (96-dimensional f32,
`m=16`, base degree limit 32, construction beam 64, search beam 12). Half
the vector clusters have nearby node IDs, half have interleaved IDs. Score
existing stored vectors against deterministic queries drawn from the dataset.
Record the nearest-self count and a digest of every ordered result ID and
distance bit pattern. Construction, warm-up, and query generation are outside
the latency window. Time the full public search call, including its snapshot.
A separate freshly opened, smaller-cache workload samples actual Linux
`read_bytes`. Read-ahead, OS-cache hits and engine-cache hits can coexist in a
query. Record the fraction of requests with at least 4 KiB of charged reads.

Compare scalar pinned decoding with fetching up to 32 vectors from each
eligible neighbor list. Use the same graph rows, layout, key traces,
release compiler and affinity within each pair. Alternate order and interleave
five independent A/A and five A/B pairs. Each process issues 10,000 queries.
Summarize the median of within-pair p99 gains. Promote the default strategy
when the primary gain reaches 10%, exceeds the largest absolute A/A difference,
and at least four of five pairs favor it. Protect ordinary reads and singleton
lists against a 5% median regression. Record the eight-key-width intermediate
and MMDB v4 adapter as separate comparisons. The application graph and its
vectors stay identical across preparations; an engine-format conversion is
unnecessary for this isolated workload. All versioned data migration tests
remain independent of the benchmark.
