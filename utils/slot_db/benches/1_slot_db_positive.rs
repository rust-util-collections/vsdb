use criterion::{Criterion, criterion_group, criterion_main};
use rand::random;
use std::hint::black_box;
use vsdb_slot_db::SlotDB;

const DATA_SIZE: u32 = 100_000;

type V = Vec<u8>;

fn slot_db_custom(mn: u64) -> SlotDB<V> {
    let mut db = SlotDB::new(mn, false);

    (0..DATA_SIZE).for_each(|i| {
        db.insert(i as u64, i.to_be_bytes().to_vec()).unwrap();
    });

    db
}

fn query(db: &SlotDB<V>, page_size: u16) {
    let page_number = random::<u32>() % (DATA_SIZE / (page_size as u32));
    db.get_entries_by_page(page_size, page_number, false);
}

fn slot_4(c: &mut Criterion) {
    let mut db = slot_db_custom(4);
    let mut group = c.benchmark_group("slot 4");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    group.bench_function("page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    group.bench_function("page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    group.bench_function("page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    group.finish();
    db.clear();
}

fn slot_8(c: &mut Criterion) {
    let mut db = slot_db_custom(8);
    let mut group = c.benchmark_group("slot 8");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    group.bench_function("page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    group.bench_function("page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    group.bench_function("page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    group.finish();
    db.clear();
}

fn slot_16(c: &mut Criterion) {
    let mut db = slot_db_custom(16);
    let mut group = c.benchmark_group("slot 16");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    group.bench_function("page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    group.bench_function("page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    group.bench_function("page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    group.finish();
    db.clear();
}

fn slot_32(c: &mut Criterion) {
    let mut db = slot_db_custom(32);
    let mut group = c.benchmark_group("slot 32");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    group.bench_function("page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    group.bench_function("page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    group.bench_function("page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    group.finish();
    db.clear();
}

fn slot_64(c: &mut Criterion) {
    let mut db = slot_db_custom(64);
    let mut group = c.benchmark_group("slot 64");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    group.bench_function("page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    group.bench_function("page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    group.bench_function("page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    group.finish();
    db.clear();
}

criterion_group!(benches, slot_64, slot_32, slot_16, slot_8, slot_4);
criterion_main!(benches);
