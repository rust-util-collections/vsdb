all: lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo check --tests
	cargo check --benches
	cargo check --examples
	cargo clippy --no-default-features --features "rocks_engine,cbor_ende"
	cargo check --tests --no-default-features --features "rocks_engine,cbor_ende"
	cargo check --benches --no-default-features --features "rocks_engine,cbor_ende"
	cargo check --examples --no-default-features --features "rocks_engine,cbor_ende"

test:
	- rm -rf ~/.vsdb
	cargo test --release -- --test-threads=1
	- rm -rf ~/.vsdb
	cargo test --release --no-default-features --features "rocks_engine,cbor_ende" -- --test-threads=1

bench:
	- rm -rf ~/.vsdb
	cargo bench
	- rm -rf ~/.vsdb
	cargo bench --no-default-features --features "rocks_engine,cbor_ende"

fmt:
	bash tools/fmt.sh

update:
	cargo update

clean:
	cargo clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
