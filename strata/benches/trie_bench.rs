use criterion::{Criterion, criterion_group, criterion_main};
use vsdb::trie::{MptCalc, SmtCalc};

fn mpt_insert(c: &mut Criterion) {
    c.bench_function("mpt_insert_100", |b| {
        b.iter(|| {
            let mut mpt = MptCalc::new();
            for i in 0u32..100 {
                mpt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
            }
        });
    });

    c.bench_function("mpt_insert_1000", |b| {
        b.iter(|| {
            let mut mpt = MptCalc::new();
            for i in 0u32..1000 {
                mpt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
            }
        });
    });
}

fn mpt_root_hash(c: &mut Criterion) {
    let mut mpt = MptCalc::new();
    for i in 0u32..1000 {
        mpt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }

    c.bench_function("mpt_root_hash_1000_cold", |b| {
        b.iter(|| {
            let mut m = mpt.clone();
            m.root_hash().unwrap()
        });
    });

    // Warm: after first hash, subsequent calls should be cheap.
    let mut warm = mpt.clone();
    warm.root_hash().unwrap();
    c.bench_function("mpt_root_hash_1000_warm", |b| {
        b.iter(|| {
            let mut m = warm.clone();
            m.root_hash().unwrap()
        });
    });
}

fn mpt_get(c: &mut Criterion) {
    let mut mpt = MptCalc::new();
    for i in 0u32..1000 {
        mpt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }

    c.bench_function("mpt_get_1000", |b| {
        b.iter(|| {
            for i in 0u32..1000 {
                mpt.get(&i.to_be_bytes()).unwrap();
            }
        });
    });
}

fn smt_insert(c: &mut Criterion) {
    c.bench_function("smt_insert_100", |b| {
        b.iter(|| {
            let mut smt = SmtCalc::new();
            for i in 0u32..100 {
                smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
            }
        });
    });

    c.bench_function("smt_insert_1000", |b| {
        b.iter(|| {
            let mut smt = SmtCalc::new();
            for i in 0u32..1000 {
                smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
            }
        });
    });
}

fn smt_root_hash(c: &mut Criterion) {
    let mut smt = SmtCalc::new();
    for i in 0u32..1000 {
        smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }

    c.bench_function("smt_root_hash_1000_cold", |b| {
        b.iter(|| {
            let mut s = smt.clone();
            s.root_hash().unwrap()
        });
    });

    let mut warm = smt.clone();
    warm.root_hash().unwrap();
    c.bench_function("smt_root_hash_1000_warm", |b| {
        b.iter(|| {
            let mut s = warm.clone();
            s.root_hash().unwrap()
        });
    });
}

fn smt_get(c: &mut Criterion) {
    let mut smt = SmtCalc::new();
    for i in 0u32..1000 {
        smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }

    c.bench_function("smt_get_1000", |b| {
        b.iter(|| {
            for i in 0u32..1000 {
                smt.get(&i.to_be_bytes()).unwrap();
            }
        });
    });
}

fn smt_prove_verify(c: &mut Criterion) {
    let mut smt = SmtCalc::new();
    for i in 0u32..100 {
        smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
    let root = smt.root_hash().unwrap();
    let root32: [u8; 32] = root.try_into().unwrap();

    c.bench_function("smt_prove_100", |b| {
        b.iter(|| {
            for i in 0u32..100 {
                smt.prove(&i.to_be_bytes()).unwrap();
            }
        });
    });

    let proofs: Vec<_> = (0u32..100)
        .map(|i| smt.prove(&i.to_be_bytes()).unwrap())
        .collect();

    c.bench_function("smt_verify_100", |b| {
        b.iter(|| {
            for proof in &proofs {
                SmtCalc::verify_proof(&root32, proof).unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    mpt_insert,
    mpt_root_hash,
    mpt_get,
    smt_insert,
    smt_root_hash,
    smt_get,
    smt_prove_verify,
);
criterion_main!(benches);
