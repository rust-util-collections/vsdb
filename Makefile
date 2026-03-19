all: fmt lint test

all-mmdb: fmt lint-mmdb test-mmdb

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

# ---- Codec feature matrix validation ----

lint-codecs:
	cargo check -p vsdb --no-default-features --features "backend_mmdb,msgpack_codec"
	cargo check -p vsdb --no-default-features --features "backend_mmdb,cbor_codec"
	@if cargo check -p vsdb --no-default-features \
	    --features "backend_mmdb,msgpack_codec,cbor_codec" 2>/dev/null; then \
		echo "expected failure when both msgpack_codec and cbor_codec are enabled"; \
		exit 1; \
	fi

# ---- MMDB backend targets ----

MMDB_FLAGS := --no-default-features --features "backend_mmdb,msgpack_codec"

lint-mmdb:
	cargo clippy --workspace $(MMDB_FLAGS)
	cargo check --workspace $(MMDB_FLAGS) --tests
	cargo check --workspace $(MMDB_FLAGS) --benches

test-mmdb:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release $(MMDB_FLAGS) --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace $(MMDB_FLAGS) --tests -- --test-threads=1

bench-mmdb:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace $(MMDB_FLAGS)
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
