all: fmt lint test

export CARGO_NET_GIT_FETCH_WITH_CLI = true

# ---- Main targets (default: MMDB backend) ----

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
	cargo bench -p vsdb_core --bench basic
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench -p vsdb --bench basic
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench -p vsdb --bench versioned
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench -p vsdb --bench slotdex
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench -p vsdb --bench trie_bench

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
