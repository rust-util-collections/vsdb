all: fmt lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo clippy --workspace --features "compress"
	cargo check --workspace --tests
	cargo check --workspace --benches
	cargo check --workspace --examples

lintall: lint
	cargo clippy --workspace --no-default-features --features "derive,rocks_engine,compress,msgpack_codec"
	cargo check --workspace --tests --no-default-features --features "derive,rocks_engine,msgpack_codec"
	cargo check --workspace --benches --no-default-features --features "derive,rocks_engine,msgpack_codec"
	cargo check --workspace --examples --no-default-features --features "derive,rocks_engine,msgpack_codec"

example:
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

test:
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --workspace --tests --bins --features "derive" -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --workspace --release --tests --bins --features "derive,compress" -- --test-threads=1

exampleall: example
	- rm -rf ~/.vsdb /tmp/vsdb_testing
	cargo run --no-default-features --features "derive,rocks_engine,msgpack_codec" --example derive_vs
	cargo run --no-default-features --features "derive,rocks_engine,msgpack_codec" --example web_server
	cargo run --no-default-features --features "derive,rocks_engine,msgpack_codec" --example blockchain_state

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --workspace --tests --bins --no-default-features --features "derive,rocks_engine,msgpack_codec" -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --workspace --release --tests --bins --no-default-features --features "derive,rocks_engine,msgpack_codec,compress" -- --test-threads=1

bench:
	- rm -rf ~/.vsdb
	cargo bench --workspace
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb
	cargo bench --workspace --features "compress"
	du -sh ~/.vsdb

benchall: bench
	- rm -rf ~/.vsdb
	cargo bench --workspace --no-default-features --features "rocks_engine,msgpack_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb
	cargo bench --workspace --no-default-features --features "rocks_engine,msgpack_codec,compress"
	du -sh ~/.vsdb

fmt:
	cargo +nightly fmt

fmtall:
	bash tools/fmt.sh

update:
	cargo update --workspace

clean:
	cargo clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
