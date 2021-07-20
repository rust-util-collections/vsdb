![](https://tokei.rs/b1/github/ccmlm/vsdb)
![GitHub top language](https://img.shields.io/github/languages/top/ccmlm/vsdb)
![GitHub issues](https://img.shields.io/github/issues-raw/ccmlm/vsdb)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ccmlm/vsdb)

# VSDB

VSDB, **V**ersioned **S**tateful **D**ata**B**ase, mainly used in blockchain scene.

Support some GIT-like operations, such as:

- Support rolling back a 'branch' to a specified historical 'version'
- Support querying the historical value of a key on the specified 'branch'

All data is divided into two dimensions: 'branch' and 'version', the functions of the 'basic' category are stateless, and the functions of the 'versioned' category are stateful.

In the internal implementation, each stateful function is implemented based on its corresponding stateless function,
all stateful data has two additional identification dimensions ('branch' and 'version'), somewhat like the logic in GIT.

Stateless functions do not have the feature of 'version' management, but they have higher performance.
