all: fmt lint test

export CARGO_NET_GIT_FETCH_WITH_CLI = true

# ---- Pre-built RocksDB C++ cache ----
#
# The C++ part of librocksdb-sys takes ~50s to compile from source.
# Cargo recompiles it whenever profiles or flags change.
#
# Fix: compile once → cache the static lib → all subsequent cargo
# invocations skip C++ entirely (build.rs sees ROCKSDB_LIB_DIR and
# only generates Rust bindings, which is instant).
#
# The cache lives outside target/ so `cargo clean` doesn't wipe it.
# Run `make clean-rocksdb` to force a fresh C++ rebuild.
#
# On FreeBSD librocksdb-sys always uses the system library and never
# compiles from source, so the cache is neither needed nor produced.
# The bootstrap detects this and becomes a no-op.

ROCKSDB_CACHE_DIR := $(HOME)/.local/rocksdb
ROCKSDB_LIB      := $(ROCKSDB_CACHE_DIR)/librocksdb.a

IS_MUSL := $(shell rustc -vV 2>/dev/null | grep -i host | grep -i musl > /dev/null && echo 1 || echo 0)

# Only export ROCKSDB_LIB_DIR when the cached library actually exists.
# This avoids pointing the linker at a nonexistent file on first run
# or on platforms where the bootstrap is a no-op (FreeBSD).
ifneq ($(IS_MUSL),1)
ifneq ($(wildcard $(ROCKSDB_LIB)),)
export ROCKSDB_LIB_DIR := $(ROCKSDB_CACHE_DIR)
export ROCKSDB_STATIC  := 1
endif
ROCKSDB_DEP := $(ROCKSDB_LIB)
else
ROCKSDB_DEP :=
endif

# One-time bootstrap: force source compilation, then cache the .a.
# On FreeBSD the .a is not produced (system library is used), so the
# cp is conditional — the target still succeeds and creates the
# directory as a marker.
$(ROCKSDB_LIB):
	@printf '\n>>> RocksDB C++ not cached — building once (~50 s) ...\n\n'
	@mkdir -p $(ROCKSDB_CACHE_DIR)
	ROCKSDB_COMPILE=1 cargo build --release -p vsdb_core
	@if ls target/release/build/librocksdb-sys-*/out/librocksdb.a >/dev/null 2>&1; then \
		cp "$$(ls -t target/release/build/librocksdb-sys-*/out/librocksdb.a | head -1)" $@; \
		printf '\n>>> Cached at %s\n\n' "$@"; \
	else \
		printf '\n>>> System librocksdb used (no .a to cache) — skipping\n\n'; \
		touch $@; \
	fi

setup-rocksdb: $(ROCKSDB_DEP)

clean-rocksdb:
	rm -rf $(ROCKSDB_CACHE_DIR)

# ---- Main targets ----

lint: $(ROCKSDB_DEP)
	cargo clippy --workspace
	cargo check --workspace --tests
	cargo check --workspace --benches

test: $(ROCKSDB_DEP)
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests -- --test-threads=1

bench: $(ROCKSDB_DEP)
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb

fmt:
	cargo fmt

fmtall:
	bash tools/fmt.sh

update:
	cargo update --verbose

clean:
	cargo clean

clean_all: clean
	git stash
	git clean -fdx

doc:
	cargo doc --open

publish:
	- cargo publish -p vsdb_core
	- cargo publish -p vsdb

publish_all: publish
	- cargo publish -p vsdb_slot_db
	- cargo publish -p vsdb_trie_db
