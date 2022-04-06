all: release

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy --features "compress"
	cargo check --tests
	cargo check --benches
	cargo check --examples

lintall: lint
	cargo clippy --no-default-features --features "derive,merkle,rocks_engine,compress,msgpack_codec"
	cargo check --tests --no-default-features --features "derive,merkle,rocks_engine,msgpack_codec"
	cargo check --benches --no-default-features --features "derive,merkle,rocks_engine,msgpack_codec"
	cargo check --examples --no-default-features --features "derive,merkle,rocks_engine,msgpack_codec"

test:
	- rm -rf ~/.vsdb
	cargo test --features "derive,merkle" -- --test-threads=1

testall: test
	- rm -rf ~/.vsdb
	cargo test --no-default-features --features "derive,merkle,rocks_engine,msgpack_codec" -- --test-threads=1

bench:
	- rm -rf ~/.vsdb
	cargo bench
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb
	cargo bench --features "compress"
	du -sh ~/.vsdb

benchall: bench
	- rm -rf ~/.vsdb
	cargo bench --no-default-features --features "rocks_engine,msgpack_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb
	cargo bench --no-default-features --features "rocks_engine,msgpack_codec,compress"
	du -sh ~/.vsdb

fmt:
	cargo +nightly fmt

fmtall:
	bash tools/fmt.sh

update:
	cargo update

clean:
	cargo clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
