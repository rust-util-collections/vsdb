![](https://tokei.rs/b1/github/FindoraNetwork/fbnc)
![GitHub top language](https://img.shields.io/github/languages/top/FindoraNetwork/fbnc)
![GitHub issues](https://img.shields.io/github/issues-raw/FindoraNetwork/fbnc)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/FindoraNetwork/fbnc)

# fBNC

fBNC, Blockchain Native Cache.

A native stateless storage library for block chain.

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
 TOML                    2           37           29            3            5
-------------------------------------------------------------------------------
 Markdown                1           17            0           10            7
 |- Shell                1           34           29            2            3
 (Total)                             51           29           12           10
-------------------------------------------------------------------------------
 Rust                   12         1729         1406           84          239
 |- Markdown            11          129            0          121            8
 (Total)                           1858         1406          205          247
===============================================================================
 Total                  16         1812         1456           97          259
===============================================================================
```
