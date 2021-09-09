![](https://tokei.rs/b1/github/FindoraNetwork/BNC)

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
 Makefile                1           29           21            0            8
 TOML                    2           31           22            3            6
-------------------------------------------------------------------------------
 Markdown                1           24            0           14           10
 |- Shell                1           62           57            2            3
 (Total)                             86           57           16           13
-------------------------------------------------------------------------------
 Rust                   10         1606         1315           82          209
 |- Markdown             9          130            0          122            8
 (Total)                           1736         1315          204          217
===============================================================================
 Total                   0         1690         1358           99          233
===============================================================================
```
