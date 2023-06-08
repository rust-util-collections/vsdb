all: fmt lintall testall

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo clippy --workspace --features "vs,extra_types"
	cargo check --workspace --tests --features "vs,extra_types"
	cargo check --workspace --benches --features "vs,extra_types"
	cargo check --workspace --examples --features "vs,extra_types"

lintall: lint
	cargo clippy --workspace --no-default-features --features "vs,sled_engine,msgpack_codec"
	cargo clippy --workspace --no-default-features --features "vs,sled_engine,compress,bcs_codec"
	cargo check --workspace --tests --no-default-features --features "vs,sled_engine,bcs_codec,extra_types"

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --features "vs" -- --test-threads=1 #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests --features "vs" -- --test-threads=1 #--nocapture

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --no-default-features --features "vs,rocks_engine,msgpack_codec" -- --test-threads=1 #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --no-default-features --features "vs,sled_engine,bcs_codec,compress" -- --test-threads=1 #--nocapture

example:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

exampleall: example
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo run --no-default-features --features "vs,sled_engine,bcs_codec" --example derive_vs
	cargo run --no-default-features --features "vs,sled_engine,bcs_codec" --example web_server
	cargo run --no-default-features --features "vs,sled_engine,bcs_codec" --example blockchain_state

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --features "compress"
	du -sh ~/.vsdb

benchall: bench
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "sled_engine,bcs_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "sled_engine,bcs_codec,compress"
	du -sh ~/.vsdb

fmt:
	cargo +nightly fmt

fmtall:
	bash scripts/fmt.sh

update:
	cargo update

clean:
	cargo clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
