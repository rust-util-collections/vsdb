all: lint

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo clippy --tests

test:
	cargo test
	cargo test --release

bench:
	cargo bench

fmt:
	@ cargo fmt
