use super::*;
use crate::StorageErrorKind;
use std::collections::BTreeMap;

fn wal_files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|file| file.unwrap().path())
        .filter(|file| file.extension().is_some_and(|ext| ext == "wal"))
        .map(|file| {
            let contents = fs::read(&file).unwrap();
            (file, contents)
        })
        .collect()
}

#[test]
fn native_shard_fence_and_admission_preserve_wal_and_sequence() {
    let dir = std::env::temp_dir()
        .join(format!("vsdb-v5-admission-{}", rand::random::<u128>()));
    let options = DbOptions::default()
        .with_prefix_len(PREFIX_SIZE)
        .with_l0_compaction_trigger(usize::MAX)
        .with_l0_slowdown_trigger(3)
        .with_l0_stop_trigger(3);
    let db = DB::open(options.clone(), &dir).unwrap();
    let engine = MmDB {
        dbs: vec![db].into_boxed_slice(),
        read_only: false,
        mem_budget: 8 << 20,
    };
    let prefix = 42u64.to_le_bytes();
    let db = engine.shard(&prefix);

    for key in [b"a", b"b", b"c"] {
        let mut batch = engine.batch_begin(Ok(prefix));
        batch.insert(key, key);
        batch.commit().unwrap();
        db.flush().unwrap();
    }
    assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("3"));
    let sequence = db.snapshot().sequence();
    let logs = wal_files(&dir);
    assert!(!logs.is_empty());
    for sync in [false, true] {
        let mut batch = engine.batch_begin(Ok(prefix));
        batch.insert(b"rejected", b"value");
        let err = batch.commit_no_slowdown(sync).unwrap_err();
        assert!(
            matches!(&err, VsdbError::Storage { kind: StorageErrorKind::Busy, source }
            if source.downcast_ref::<mmdb::Error>().is_some_and(|e| e.kind() == mmdb::ErrorKind::Busy))
        );
    }
    assert_eq!(db.snapshot().sequence(), sequence);
    assert_eq!(wal_files(&dir), logs);
    assert_eq!(engine.get(prefix, b"rejected"), None);

    db.compact().unwrap();
    let mut retry = engine.batch_begin(Ok(prefix));
    retry.insert(b"rejected", b"accepted");
    retry.commit_no_slowdown(false).unwrap();
    let accepted = db.snapshot().sequence();
    assert_eq!(accepted, sequence + 1);
    let logs = wal_files(&dir);
    engine.try_sync_wal(prefix).unwrap();
    engine.try_sync_all_wals().unwrap();
    assert_eq!(db.snapshot().sequence(), accepted);
    assert_eq!(wal_files(&dir), logs);

    let mut malformed = engine.batch_begin(Ok(prefix));
    malformed.remove(&vec![0; mmdb::MAX_USER_KEY_SIZE]);
    let error = malformed.commit_no_slowdown(false).unwrap_err();
    assert!(matches!(
        error,
        VsdbError::Storage {
            kind: StorageErrorKind::InvalidArgument,
            ..
        }
    ));
    assert_eq!(db.snapshot().sequence(), accepted);
    assert_eq!(wal_files(&dir), logs);
    engine.close().unwrap();
    {
        let reopened = DB::open(options, &dir).unwrap();
        for key in [b"a", b"b", b"c"] {
            assert_eq!(
                reopened
                    .get(&make_full_key(&prefix, key))
                    .unwrap()
                    .as_deref(),
                Some(key.as_slice())
            );
        }
        assert_eq!(
            reopened
                .get(&make_full_key(&prefix, b"rejected"))
                .unwrap()
                .as_deref(),
            Some(b"accepted".as_slice())
        );
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn namespace_budget_counts_all_five_possible_generations_and_both_stores() {
    let normal = EngineSizing::from_budget_mb(2048);
    assert_eq!(normal.write_buffer_size(16), 16 << 20);
    assert_eq!(normal.write_buffer_size(1), 256 << 20);
    assert_eq!(normal.pool_store_capacity(), 256 << 20);
    for mb in [0, 1, 7, 8] {
        let named = EngineSizing::from_budget_mb(mb);
        assert_eq!(named.mem_budget, 8 << 20);
        assert_eq!(named.write_buffer_size(64), 64 << 10);
        assert_eq!(named.pool_store_capacity(), 1 << 20);
        let expected_default = if mb == 0 { 2048 } else { mb };
        assert_eq!(effective_mem_budget(Some(mb)), expected_default << 20);
    }
    let small = EngineSizing::from_budget_mb(1);
    assert_eq!(small.pool_store_capacity(), 1 << 20);
    assert_eq!(small.write_buffer_size(1), 1 << 20);
    assert_eq!(small.write_buffer_size(16), 64 << 10);
    assert_eq!(
        EngineSizing::from_budget_mb(usize::MAX).write_buffer_size(1),
        512 << 20
    );
}

#[test]
fn full_immutable_queue_returns_busy_without_writing_the_refused_batch() {
    use std::{sync::mpsc, time::Duration};

    type Signals = Arc<Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>>;
    struct ParkFirstFlush(Signals);
    impl mmdb::BlockPropertyCollector for ParkFirstFlush {
        fn add(&mut self, _: &[u8]) {
            if let Some((entered, release)) = self.0.lock().take() {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }
        fn finish_block(&mut self) -> Vec<u8> {
            Vec::new()
        }
        fn name(&self) -> &str {
            "vsdb-admission-test"
        }
    }

    let dir = std::env::temp_dir()
        .join(format!("vsdb-v5-immutables-{}", rand::random::<u128>()));
    let (enter_tx, enter_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let signals = Arc::new(Mutex::new(Some((enter_tx, release_rx))));
    let options = DbOptions::default()
        .with_prefix_len(PREFIX_SIZE)
        .with_write_buffer_size(1024)
        .with_max_immutable_memtables(1)
        .with_block_property_collectors(vec![Arc::new(move || {
            Box::new(ParkFirstFlush(Arc::clone(&signals)))
        })]);
    let engine = MmDB {
        dbs: vec![DB::open(options, &dir).unwrap()].into_boxed_slice(),
        read_only: false,
        mem_budget: 8 << 20,
    };
    let prefix = 42u64.to_le_bytes();
    let mut accepted = engine.batch_begin(Ok(prefix));
    accepted.insert(b"older", &[9; 4096]);
    accepted.commit().unwrap();
    enter_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let db = engine.shard(&prefix);
    let usage = db.memory_usage();
    let sequence = db.snapshot().sequence();
    let logs = wal_files(&dir);
    let mut refused = engine.batch_begin(Ok(prefix));
    refused.insert(b"newer", b"row");
    let result = refused.commit_no_slowdown(true);
    let after = db.snapshot().sequence();
    let after_logs = wal_files(&dir);
    let visible = engine.get(prefix, b"newer");
    release_tx.send(()).unwrap();
    assert_eq!(usage.immutable_memtable_count, 1);
    assert!(usage.immutable_memtable_bytes > 0);
    assert!(matches!(
        result,
        Err(VsdbError::Storage {
            kind: StorageErrorKind::Busy,
            ..
        })
    ));
    assert_eq!(after, sequence);
    assert_eq!(after_logs, logs);
    assert_eq!(visible, None);
    db.flush().unwrap();
    let mut retry = engine.batch_begin(Ok(prefix));
    retry.insert(b"newer", b"row");
    retry.commit_no_slowdown(true).unwrap();
    assert_eq!(db.snapshot().sequence(), sequence + 1);
    assert_eq!(
        engine.get(prefix, b"newer").as_deref(),
        Some(b"row".as_slice())
    );
    engine.close().unwrap();
    fs::remove_dir_all(dir).unwrap();
}
