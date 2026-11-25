all: fmt lint test

export CARGO_NET_GIT_FETCH_WITH_CLI = true

# ---- Main targets ----

lint:
	cargo clippy --workspace
	cargo check --workspace --tests
	cargo check --workspace --benches
	$(MAKE) check-make-isolation

# Guard the test/bench recipes against deleting user or shared data again.
check-make-isolation:
	@if $(MAKE) -n test bench | grep -E '(~/\.vsdb|/tmp/vsdb_testing|rm -rf .*VSDB_BASE_DIR)'; then \
		echo "make test/bench must not delete user or shared data" >&2; exit 1; \
	fi
	@$(MAKE) -n test bench | grep -q 'mktemp -d /tmp/vsdb-' || \
		(echo "make test/bench must use a per-invocation scratch base" >&2; exit 1)

test:
	@scratch=$$(mktemp -d /tmp/vsdb-test.XXXXXX) || exit 1; \
	trap 'rm -rf "$$scratch"' EXIT; \
	mkdir "$$scratch/tmp" || exit 1; \
	export TMPDIR="$$scratch/tmp" VSDB_BASE_DIR="$$scratch/universe"; \
	cargo test --workspace --release --tests && \
	cargo test --workspace --tests && \
	cargo test --workspace --doc

bench:
	@scratch=$$(mktemp -d /tmp/vsdb-bench.XXXXXX) || exit 1; \
	trap 'rm -rf "$$scratch"' EXIT; \
	mkdir "$$scratch/tmp" || exit 1; \
	export TMPDIR="$$scratch/tmp" VSDB_BASE_DIR="$$scratch/universe"; \
	cargo bench -p vsdb_core --bench basic && \
	cargo bench -p vsdb_core --bench cache_pool && \
	cargo bench -p vsdb --bench basic && \
	cargo bench -p vsdb --bench versioned && \
	cargo bench -p vsdb --bench slotdex && \
	cargo bench -p vsdb --bench slotdex_batch && \
	cargo bench -p vsdb --bench dagmap && \
	cargo bench -p vsdb --bench trie_bench && \
	cargo bench -p vsdb --bench vecdex

# ---- Utilities ----

fmt:
	cargo fmt

fmtall:
	bash tools/fmt.sh

update:
	cargo update --verbose

clean:
	cargo clean

clean_all: clean
	git stash
	git clean -fdx

doc:
	cargo doc --open

publish:
	- cargo publish -p vsdb_core
	- cargo publish -p vsdb
