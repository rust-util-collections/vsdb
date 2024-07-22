all: fmt lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo clippy --workspace --features "vs,bcs_codec,compress,extra_types"
	cargo check --workspace --tests --features "bcs_codec"
	cargo check --workspace --benches --features "bcs_codec"
	cargo check --workspace --examples --features "bcs_codec"

lintall: lint
	cargo clippy --workspace --no-default-features --features "vs,rocks_engine,compress,json_codec"
	cargo clippy --workspace --no-default-features --features "vs,rocks_engine,compress,msgpack_codec"
	cargo check --workspace --tests --no-default-features --features "vs,rocks_engine,msgpack_codec,extra_types"
	cargo check --workspace --benches --no-default-features --features "vs,rocks_engine,msgpack_codec"
	cargo check --workspace --examples --no-default-features --features "vs,rocks_engine,msgpack_codec"

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo test --workspace --release --tests --features "vs,compress,bcs_codec" -- --test-threads=1 #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo test --workspace --tests --features "vs,json_codec" -- --test-threads=1 #--nocapture

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo test --workspace --release --tests --no-default-features --features "vs,rocks_engine,msgpack_codec,compress" -- --test-threads=1 #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo test --workspace --release --tests --no-default-features --features "vs,rocks_engine,json_codec,compress" -- --test-threads=1 #--nocapture
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo test --workspace --tests --no-default-features --features "vs,rocks_engine,bcs_codec" -- --test-threads=1 #--nocapture

example:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

exampleall: example
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo run --no-default-features --features "vs,rocks_engine,msgpack_codec" --example derive_vs
	cargo run --no-default-features --features "vs,rocks_engine,msgpack_codec" --example web_server
	cargo run --no-default-features --features "vs,rocks_engine,msgpack_codec" --example blockchain_state

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo bench --workspace
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo bench --workspace --features "compress"
	du -sh ~/.vsdb

benchall: bench
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo bench --workspace --no-default-features --features "rocks_engine,msgpack_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing
	cargo bench --workspace --no-default-features --features "rocks_engine,msgpack_codec,compress"
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
