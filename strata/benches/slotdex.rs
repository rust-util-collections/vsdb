use criterion::{Criterion, criterion_group, criterion_main};
use rand::random;
use std::hint::black_box;
use vsdb::slotdex::SlotDex;

const DATA_SIZE: u32 = 100_000;

type V = Vec<u8>;

fn slot_db_custom(mn: u64) -> SlotDex<V> {
    let mut db = SlotDex::new(mn, false);

    (0..DATA_SIZE).for_each(|i| {
        db.insert(i as u64, i.to_be_bytes().to_vec()).unwrap();
    });

    db
}

fn query(db: &SlotDex<V>, page_size: u16) {
    let page_number = random::<u32>() % (DATA_SIZE / (page_size as u32));
    db.get_entries_by_page(page_size, page_number, false);
}

fn query_reverse(db: &SlotDex<V>, page_size: u16) {
    let page_number = random::<u32>() % (DATA_SIZE / (page_size as u32));
    db.get_entries_by_page(page_size, page_number, true);
}

fn slot_query(c: &mut Criterion, tier: u64) {
    let mut db = slot_db_custom(tier);
    let name = format!("slot {tier}");
    let mut group = c.benchmark_group(&name);
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    for &ps in &[10u16, 20, 40, 80] {
        group.bench_function(format!("page size: {ps}"), |b| {
            b.iter(|| query(&db, black_box(ps)))
        });
    }

    for &ps in &[10u16, 40] {
        group.bench_function(format!("page size: {ps} (reverse)"), |b| {
            b.iter(|| query_reverse(&db, black_box(ps)))
        });
    }

    group.finish();
    db.clear();
}

fn slot_4(c: &mut Criterion) {
    slot_query(c, 4);
}
fn slot_8(c: &mut Criterion) {
    slot_query(c, 8);
}
fn slot_16(c: &mut Criterion) {
    slot_query(c, 16);
}
fn slot_32(c: &mut Criterion) {
    slot_query(c, 32);
}
fn slot_64(c: &mut Criterion) {
    slot_query(c, 64);
}

fn slot_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("slot write");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    group.bench_function("insert", |b| {
        let mut db: SlotDex<V> = SlotDex::new(16, false);
        let mut i = 0u64;
        b.iter(|| {
            db.insert(i, i.to_be_bytes().to_vec()).unwrap();
            i += 1;
        });
        db.clear();
    });

    group.bench_function("remove", |b| {
        let mut db: SlotDex<V> = SlotDex::new(16, false);
        // Pre-populate
        for i in 0..DATA_SIZE as u64 {
            db.insert(i, i.to_be_bytes().to_vec()).unwrap();
        }
        let mut i = 0u64;
        b.iter(|| {
            db.remove(i, &i.to_be_bytes().to_vec());
            i += 1;
        });
        db.clear();
    });

    group.finish();
}

criterion_group!(
    benches, slot_64, slot_32, slot_16, slot_8, slot_4, slot_write
);
criterion_main!(benches);
