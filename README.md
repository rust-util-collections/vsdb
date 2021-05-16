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
 Markdown                1           17            0           10            7
 |- Shell                1           34           29            2            3
 (Total)                             51           29           12           10
-------------------------------------------------------------------------------
 Rust                   10         1833         1520           89          224
 |- Markdown             9          133            0          125            8
 (Total)                           1966         1520          214          232
===============================================================================
 Total                  14         1910         1563          102          245
===============================================================================
```
