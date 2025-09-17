all: fmt lintall testall

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo check --workspace --tests
	cargo check --workspace --benches

lintall: lint
	cargo clippy --workspace --no-default-features --features "rocks_backend,cbor_codec"
	cargo check --workspace --tests --no-default-features --features "rocks_backend,json_codec"

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests -- --test-threads=1

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests \
		--no-default-features \
		--features "rocks_backend,cbor_codec" \
		-- --test-threads=1 #--nocapture

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb

bench_rocksdb:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "rocks_backend,cbor_codec"
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
