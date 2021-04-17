# BNC

BNC, Blockchain Native Cache.

A native mix-storage('memory + disk') library for block chain.

Its value is to improve the stability and security of online services, at the cost of some single-node performance losses.

## Code Structure

```shell
# zsh % tree -F src

src
├── helper.rs
├── lib.rs
├── mapx/
│   ├── backend.rs
│   ├── mod.rs
│   └── test.rs
├── serde.rs
└── vecx/
    ├── backend.rs
    ├── mod.rs
    └── test.rs

2 directories, 9 files
```

```shell
# zsh % tokei

===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Makefile                1           21           15            0            6
 TOML                    2           29           22            3            4
-------------------------------------------------------------------------------
 Markdown                1           22            0           13            9
 |- Shell                1           66           61            2            3
 (Total)                             88           61           15           12
-------------------------------------------------------------------------------
 Rust                   10         1490         1227           73          190
 |- Markdown             9          127            0          118            9
 (Total)                           1617         1227          191          199
===============================================================================
 Total                   0         1562         1264           89          209
===============================================================================
```

## Benchmark

> **The benches are running on a SSD disk.**

```shell
Benchmarking ** Cache DB Benchmark **/vecx_write: Collecting 200 samples in estimated 10.316 s (603k iterations)
** Cache DB Benchmark **/vecx_write
                        time:   [38.726 us 40.254 us 41.757 us]
                        change: [-81.977% -68.235% -23.844%] (p = 0.03 < 0.05)
                        Performance has improved.
Found 7 outliers among 200 measurements (3.50%)
  7 (3.50%) high mild
** Cache DB Benchmark **/vecx_rw
                        time:   [67.771 us 71.981 us 76.136 us]
                        change: [-75.119% -60.941% -27.112%] (p = 0.01 < 0.05)
                        Performance has improved.
** Cache DB Benchmark **/mapx_write
                        time:   [38.376 us 39.937 us 41.449 us]
                        change: [-87.222% -68.261% +14.016%] (p = 0.60 > 0.05)
                        No change in performance detected.
Found 5 outliers among 200 measurements (2.50%)
  5 (2.50%) high mild
** Cache DB Benchmark **/mapx_rw
                        time:   [68.660 us 72.826 us 77.023 us]
                        change: [-40.155% +20.639% +145.50%] (p = 0.77 > 0.05)
                        No change in performance detected.
** Cache DB Benchmark **/mapx_mut_back
                        time:   [47.054 us 47.508 us 47.949 us]
                        change: [-94.825% -89.170% -43.483%] (p = 0.04 < 0.05)
                        Performance has improved.
Found 9 outliers among 200 measurements (4.50%)
  8 (4.00%) low mild
  1 (0.50%) high mild
```
