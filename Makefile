all: lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo clippy --tests
	cargo clippy --benches
	cargo clippy --no-default-features --features rocks_engine
	cargo clippy --tests --no-default-features --features rocks_engine
	cargo clippy --benches --no-default-features --features rocks_engine

test:
	- rm -rf ~/.vsdb
	cargo test --release -- --test-threads=1
	- rm -rf ~/.vsdb
	cargo test --release --no-default-features --features rocks_engine -- --test-threads=1

bench:
	- rm -rf ~/.vsdb
	cargo bench
	- rm -rf ~/.vsdb
	cargo bench --no-default-features --features rocks_engine

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
