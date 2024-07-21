all: fmt lint test

fmt:
	cargo fmt

lint:
	cargo clippy --workspace
	cargo clippy --workspace --tests
	@ # cargo clippy --workspace --examples
	@ # cargo clippy --workspace --features="benchmark"

musl_lint:
	if [ `uname -s` = "Linux" ]; then \
		cargo clippy --workspace --target=x86_64-unknown-linux-musl; \
	fi

test:
	rm -rf ~/.vsdb
	cargo test --workspace -- --nocapture

update:
	rustup update stable
	cargo update

clean:
	cargo clean
