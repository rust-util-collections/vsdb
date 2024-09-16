all: fmt lintall testall

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo clippy --workspace --features "vs,extra_types"
	cargo check --workspace --tests --features "vs,extra_types"
	cargo check --workspace --benches --features "vs,extra_types"
	cargo check --workspace --examples --features "vs,extra_types"

lintall: lint
	cargo clippy --workspace --no-default-features --features "vs,msgpack_codec"
	cargo clippy --workspace --no-default-features --features "vs,compress,bcs_codec"
	cargo check --workspace --tests --no-default-features --features "vs,json_codec,extra_types"

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --features "vs" #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests --features "vs" #--nocapture

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --no-default-features --features "vs,msgpack_codec" #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests --no-default-features --features "vs,bcs_codec,compress" #--nocapture

example:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --features "compress"
	du -sh ~/.vsdb

fmt:
	cargo +nightly fmt

fmtall:
	bash scripts/fmt.sh

update:
	cargo update

clean:
	cargo clean

clean_all: clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
