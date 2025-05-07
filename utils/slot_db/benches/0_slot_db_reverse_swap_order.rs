use criterion::{Criterion, criterion_group, criterion_main};
use rand::random;
use std::hint::black_box;
use vsdb_slot_db::SlotDB;

const DATA_SIZE: u32 = 100_0000;

type V = Vec<u8>;

fn slot_db_custom(mn: u64) -> SlotDB<V> {
    let mut db = SlotDB::new(mn, true);

    (1..DATA_SIZE).for_each(|i| {
        db.insert(i as u64, i.to_be_bytes().to_vec()).unwrap();
    });

    db
}

fn query(db: &SlotDB<V>, page_size: u16) {
    let page_number = random::<u32>() % (DATA_SIZE / (page_size as u32));
    db.get_entries_by_page(page_size, page_number, true);
}

fn slot_4(c: &mut Criterion) {
    let mut db = slot_db_custom(4);

    c.bench_function("slot 4, page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    c.bench_function("slot 4, page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    c.bench_function("slot 4, page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    c.bench_function("slot 4, page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    db.clear();
}

fn slot_8(c: &mut Criterion) {
    let mut db = slot_db_custom(8);
    c.bench_function("slot 8, page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    c.bench_function("slot 8, page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    c.bench_function("slot 8, page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    c.bench_function("slot 8, page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    db.clear();
}

fn slot_16(c: &mut Criterion) {
    let mut db = slot_db_custom(16);
    c.bench_function("slot 16, page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    c.bench_function("slot 16, page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    c.bench_function("slot 16, page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    c.bench_function("slot 16, page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    db.clear();
}

fn slot_32(c: &mut Criterion) {
    let mut db = slot_db_custom(32);
    c.bench_function("slot 32, page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    c.bench_function("slot 32, page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    c.bench_function("slot 32, page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    c.bench_function("slot 32, page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    db.clear();
}

fn slot_64(c: &mut Criterion) {
    let mut db = slot_db_custom(64);
    c.bench_function("slot 64, page size: 10", |b| {
        b.iter(|| query(&db, black_box(10)))
    });
    c.bench_function("slot 64, page size: 20", |b| {
        b.iter(|| query(&db, black_box(20)))
    });
    c.bench_function("slot 64, page size: 40", |b| {
        b.iter(|| query(&db, black_box(40)))
    });
    c.bench_function("slot 64, page size: 80", |b| {
        b.iter(|| query(&db, black_box(80)))
    });

    db.clear();
}

criterion_group!(benches, slot_64, slot_32, slot_16, slot_8, slot_4);
criterion_main!(benches);
