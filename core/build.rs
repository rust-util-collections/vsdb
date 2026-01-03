fn main() {
    // When using a pre-built RocksDB library (ROCKSDB_LIB_DIR is set),
    // librocksdb-sys's build.rs skips build_rocksdb() — which is where
    // the cc crate would normally emit `cargo:rustc-link-lib=c++`.
    // Without it, any binary target (test, bench, bin) fails to link
    // C++ standard library symbols.  We patch it up here.
    println!("cargo:rerun-if-env-changed=ROCKSDB_LIB_DIR");
    if std::env::var("ROCKSDB_LIB_DIR").is_ok() {
        let target = std::env::var("TARGET").unwrap_or_default();
        if target.contains("musl") {
            // On musl (Alpine Linux) platforms, we do not use the cached rocksdb lib.
            // So we don't need to link `stdc++` manually.
        } else if target.contains("linux") {
            println!("cargo:rustc-link-lib=stdc++");
        } else if !target.contains("windows") {
            println!("cargo:rustc-link-lib=c++");
        }
    }
}
