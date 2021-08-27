all: lint

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo clippy --tests
	cargo clippy --features=debug_env
	cargo clippy --tests --features=debug_env
	cargo clippy --features=compress
	cargo clippy --tests --features=compress

test:
	cargo test --release -- --test-threads=1
	cargo test --features=compress -- --test-threads=1
	cargo test --release --features=debug_env -- --test-threads=1
	cargo test --features=compress --features=debug_env -- --test-threads=1

bench:
	cargo bench
	cargo bench --features="compress"

fmt:
	@ cargo fmt

update:
	cargo update
