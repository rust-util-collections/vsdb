all: lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo clippy --tests
	cargo clippy --no-default-features
	cargo clippy --no-default-features --tests

test:
	cargo test --release -- --test-threads=1 --nocapture
	cargo test --release --no-default-features -- --test-threads=1 --nocapture

bench:
	cargo bench
	cargo bench --no-default-features

fmt:
	@ cargo fmt

update:
	cargo update

clean:
	cargo clean
	git stash
	git clean -fdx
