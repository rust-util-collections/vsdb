all: fmt lint test

all-rocksdb: fmt lint-rocksdb test-rocksdb

export CARGO_NET_GIT_FETCH_WITH_CLI = true

# ---- Main targets (default: RocksDB backend) ----

lint:
	cargo clippy --workspace
	cargo check --workspace --tests
	cargo check --workspace --benches

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests -- --test-threads=1

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb

# ---- RocksDB backend targets ----

RocksDB_FLAGS := --no-default-features --features "backend_rocksdb"

lint-rocksdb:
	cargo clippy --workspace $(RocksDB_FLAGS)
	cargo check --workspace $(RocksDB_FLAGS) --tests
	cargo check --workspace $(RocksDB_FLAGS) --benches

test-rocksdb:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release $(RocksDB_FLAGS) --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace $(RocksDB_FLAGS) --tests -- --test-threads=1

bench-rocksdb:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace $(RocksDB_FLAGS)
	du -sh ~/.vsdb

# ---- Utilities ----

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
