all: fmt lintall testall

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo clippy --workspace --features "vs,extra_types"
	cargo check --workspace --tests --features "vs,extra_types"
	cargo check --workspace --benches --features "vs,extra_types"
	cargo check --workspace --examples --features "vs,extra_types"

lintall: lint
	cargo clippy --workspace --no-default-features --features "parity_backend,vs,msgpack_codec"
	cargo clippy --workspace --no-default-features --features "parity_backend,vs,compress,msgpack_codec"
	cargo check --workspace --tests --no-default-features --features "parity_backend,vs,json_codec"

lintmusl:
	cargo clippy --workspace --target x86_64-unknown-linux-musl \
		--no-default-features \
		--features "parity_backend,vs,msgpack_codec,extra_types"

test:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --release --tests -- --test-threads=1

testall: test
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --tests \
		--no-default-features \
		--features "parity_backend,vs,msgpack_codec" \
		-- --test-threads=1 #--nocapture

testmusl:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo test --workspace --target x86_64-unknown-linux-musl --release --tests \
		--no-default-features \
		--features "parity_backend,vs,msgpack_codec" \
		-- --test-threads=1 #--nocapture

example:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

bench:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "parity_backend,msgpack_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "parity_backend,compress,msgpack_codec"
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.vsdb
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --features "compress"
	du -sh ~/.vsdb

benchmusl:
	- rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)
	cargo bench --workspace --target x86_64-unknown-linux-musl \
		--no-default-features --features "parity_backend,msgpack_codec"
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
