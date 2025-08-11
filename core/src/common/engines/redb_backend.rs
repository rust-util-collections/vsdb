use crate::common::{
    Engine, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use parking_lot::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use ruc::*;
use std::{
    borrow::Cow,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

// Table definitions for different data areas
const DATA_TABLES: [TableDefinition<&[u8], &[u8]>; DATA_SET_NUM as usize] = [
    TableDefinition::new("data_0"),
    TableDefinition::new("data_1"),
    TableDefinition::new("data_2"),
    TableDefinition::new("data_3"),
    TableDefinition::new("data_4"),
    TableDefinition::new("data_5"),
    TableDefinition::new("data_6"),
    TableDefinition::new("data_7"),
];
const META_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");

const DATA_SET_NUM: u8 = 8;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

static HDR: LazyLock<Database> = LazyLock::new(|| redb_open().unwrap());

pub struct RedbEngine {
    hdr: &'static Database,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl RedbEngine {
    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        let txn = self.hdr.begin_write().unwrap();
        {
            let mut table = txn.open_table(META_TABLE).unwrap();
            table
                .insert(&META_KEY_MAX_KEYLEN[..], &len.to_be_bytes()[..])
                .unwrap();
        }
        txn.commit().unwrap();
    }

    fn get_table_definition(
        &self,
        area_idx: usize,
    ) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        *DATA_TABLES.get(area_idx).unwrap()
    }
}

impl Engine for RedbEngine {
    fn new() -> Result<Self> {
        let hdr = &HDR;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        // Initialize metadata if not exists
        let txn = hdr.begin_write().c(d!())?;
        {
            let mut table = txn.open_table(META_TABLE).c(d!())?;

            // Check and initialize max_keylen
            if table.get(&META_KEY_MAX_KEYLEN[..]).c(d!())?.is_none() {
                table
                    .insert(&META_KEY_MAX_KEYLEN[..], &0_usize.to_be_bytes()[..])
                    .c(d!())?;
            }

            // Check and initialize prefix allocator
            if table.get(&prefix_allocator.key[..]).c(d!())?.is_none() {
                table
                    .insert(&prefix_allocator.key[..], &initial_value[..])
                    .c(d!())?;
            }
        }
        txn.commit().c(d!())?;

        // Get max_keylen from metadata
        let txn = hdr.begin_read().c(d!())?;
        let table = txn.open_table(META_TABLE).c(d!())?;
        let max_keylen_bytes = table.get(&META_KEY_MAX_KEYLEN[..]).c(d!())?.unwrap();
        let max_keylen =
            AtomicUsize::new(crate::parse_int!(max_keylen_bytes.value(), usize));

        Ok(RedbEngine {
            hdr,
            prefix_allocator,
            max_keylen,
        })
    }

    fn alloc_prefix(&self) -> Pre {
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let x = LK.lock();

        let txn = self.hdr.begin_write().unwrap();
        let ret = {
            let mut table = txn.open_table(META_TABLE).unwrap();
            let current = {
                let current_bytes =
                    table.get(&self.prefix_allocator.key[..]).unwrap().unwrap();
                crate::parse_prefix!(current_bytes.value())
            };

            table
                .insert(
                    &self.prefix_allocator.key[..],
                    &(current + 1).to_be_bytes()[..],
                )
                .unwrap();

            current
        };
        txn.commit().unwrap();

        drop(x);
        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM as usize
    }

    fn flush(&self) {
        // redb handles flushing automatically
    }

    fn iter(&self, hdr_prefix: PreBytes) -> RedbIter {
        let area_idx = self.area_idx(hdr_prefix);
        let table_def = self.get_table_definition(area_idx);

        RedbIter::new(
            self.hdr,
            table_def,
            hdr_prefix,
            Bound::Unbounded,
            Bound::Unbounded,
        )
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        hdr_prefix: PreBytes,
        bounds: R,
    ) -> RedbIter {
        let area_idx = self.area_idx(hdr_prefix);
        let table_def = self.get_table_definition(area_idx);

        let start = bounds.start_bound().map(|c| c.as_ref().to_vec());
        let end = bounds.end_bound().map(|c| c.as_ref().to_vec());
        RedbIter::new(self.hdr, table_def, hdr_prefix, start, end)
    }

    fn get(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_idx = self.area_idx(hdr_prefix);
        let table_def = self.get_table_definition(area_idx);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);

        let txn = self.hdr.begin_read().ok()?;
        let table = txn.open_table(table_def).ok()?;
        table.get(&k[..]).ok()?.map(|v| v.value().to_vec())
    }

    fn insert(
        &self,
        hdr_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let area_idx = self.area_idx(hdr_prefix);
        let table_def = self.get_table_definition(area_idx);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        let txn = self.hdr.begin_write().unwrap();
        let old_v = {
            let table = txn.open_table(table_def).unwrap();
            table.get(&k[..]).unwrap().map(|v| v.value().to_vec())
        };

        {
            let mut table = txn.open_table(table_def).unwrap();
            table.insert(&k[..], value).unwrap();
        }
        txn.commit().unwrap();

        old_v
    }

    fn remove(&self, hdr_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let area_idx = self.area_idx(hdr_prefix);
        let table_def = self.get_table_definition(area_idx);

        let mut k = hdr_prefix.to_vec();
        k.extend_from_slice(key);

        let txn = self.hdr.begin_write().unwrap();
        let old_v = {
            let table = txn.open_table(table_def).unwrap();
            table.get(&k[..]).unwrap().map(|v| v.value().to_vec())
        };

        if old_v.is_some() {
            let mut table = txn.open_table(table_def).unwrap();
            table.remove(&k[..]).unwrap();
        }
        txn.commit().unwrap();

        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        let txn = self.hdr.begin_read().unwrap();
        let table = txn.open_table(META_TABLE).unwrap();
        if let Ok(Some(bytes)) = table.get(&instance_prefix[..]) {
            crate::parse_int!(bytes.value(), u64)
        } else {
            0
        }
    }

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64) {
        let txn = self.hdr.begin_write().unwrap();
        {
            let mut table = txn.open_table(META_TABLE).unwrap();
            table
                .insert(&instance_prefix[..], &new_len.to_be_bytes()[..])
                .unwrap();
        }
        txn.commit().unwrap();
    }
}

pub struct RedbIter {
    db: &'static Database,
    table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
    prefix: Vec<u8>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    current_position: Option<Vec<u8>>,
    exhausted: bool,
    reverse_items: Option<Vec<(RawKey, RawValue)>>,
}

impl RedbIter {
    fn new(
        db: &'static Database,
        table_def: TableDefinition<'static, &'static [u8], &'static [u8]>,
        prefix: PreBytes,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Self {
        RedbIter {
            db,
            table_def,
            prefix: prefix.to_vec(),
            start_bound: start,
            end_bound: end,
            current_position: None,
            exhausted: false,
            reverse_items: None,
        }
    }

    fn check_bounds(&self, raw_key: &[u8]) -> bool {
        match (&self.start_bound, &self.end_bound) {
            (Bound::Unbounded, Bound::Unbounded) => true,
            (Bound::Included(s), Bound::Unbounded) => raw_key >= s.as_slice(),
            (Bound::Excluded(s), Bound::Unbounded) => raw_key > s.as_slice(),
            (Bound::Unbounded, Bound::Included(e)) => raw_key <= e.as_slice(),
            (Bound::Unbounded, Bound::Excluded(e)) => raw_key < e.as_slice(),
            (Bound::Included(s), Bound::Included(e)) => {
                raw_key >= s.as_slice() && raw_key <= e.as_slice()
            }
            (Bound::Included(s), Bound::Excluded(e)) => {
                raw_key >= s.as_slice() && raw_key < e.as_slice()
            }
            (Bound::Excluded(s), Bound::Included(e)) => {
                raw_key > s.as_slice() && raw_key <= e.as_slice()
            }
            (Bound::Excluded(s), Bound::Excluded(e)) => {
                raw_key > s.as_slice() && raw_key < e.as_slice()
            }
        }
    }

    fn get_next_item(&mut self) -> Option<(RawKey, RawValue)> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(self.table_def).ok()?;

        let start_key = match &self.current_position {
            Some(pos) => {
                let mut key = self.prefix.clone();
                key.extend_from_slice(pos);
                key
            }
            None => self.prefix.clone(),
        };

        let mut iter = table.range(&start_key[..]..).ok()?;

        // If we have a current position, skip the current item to get the next one
        if self.current_position.is_some() {
            iter.next();
        }

        while let Some(Ok((k, v))) = iter.next() {
            let key_bytes = k.value();
            if !key_bytes.starts_with(&self.prefix) {
                self.exhausted = true;
                return None;
            }

            let raw_key = &key_bytes[PREFIX_SIZE..];
            if self.check_bounds(raw_key) {
                self.current_position = Some(raw_key.to_vec());
                return Some((raw_key.to_vec(), v.value().to_vec()));
            }
        }

        self.exhausted = true;
        None
    }

    fn collect_all_for_reverse(&mut self) -> Option<()> {
        if self.reverse_items.is_some() {
            return Some(());
        }

        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(self.table_def).ok()?;
        let mut items = Vec::new();

        let mut iter = table.range(&self.prefix[..]..).ok()?;
        while let Some(Ok((k, v))) = iter.next() {
            let key_bytes = k.value();
            if !key_bytes.starts_with(&self.prefix) {
                break;
            }

            let raw_key = &key_bytes[PREFIX_SIZE..];
            if self.check_bounds(raw_key) {
                items.push((raw_key.to_vec(), v.value().to_vec()));
            }
        }

        self.reverse_items = Some(items);
        Some(())
    }
}

impl Iterator for RedbIter {
    type Item = (RawKey, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        self.get_next_item()
    }
}

impl DoubleEndedIterator for RedbIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.collect_all_for_reverse()?;

        self.reverse_items.as_mut()?.pop()
    }
}

// key of the prefix allocator in the 'hdr'
struct PreAllocator {
    key: [u8; 1],
}

impl PreAllocator {
    const fn init() -> (Self, PreBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Pre::MIN).to_be_bytes(),
        )
    }
}

fn redb_open() -> Result<Database> {
    let dir = vsdb_get_base_dir();

    // avoid setting again on an opened DB
    omit!(vsdb_set_base_dir(&dir));

    let db_path = dir.join("redb_data.redb");
    let db = Database::create(db_path).c(d!())?;

    // Initialize tables
    let txn = db.begin_write().c(d!())?;
    for tb in DATA_TABLES.iter() {
        txn.open_table(*tb).c(d!())?;
    }
    txn.open_table(META_TABLE).c(d!())?;
    txn.commit().c(d!())?;

    Ok(db)
}
