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

test:
	cargo test --release -- --test-threads=1

bench:
	cargo bench

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
