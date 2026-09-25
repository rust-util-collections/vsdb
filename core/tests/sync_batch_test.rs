use std::{
    env, fs,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    thread,
    time::Duration,
};
use vsdb_core::{
    InstanceId, MapxRaw, Namespace, VsdbError, VsdbOptions, vsdb_configure,
};

#[test]
fn synchronous_wipe_and_dropped_batch() {
    let mut map = MapxRaw::new();
    map.insert(b"old", b"old");
    let mut batch = map.batch_wiped();
    batch.insert(b"new", b"value");
    batch.commit_sync().unwrap();
    assert_eq!(map.get(b"old"), None);
    assert_eq!(map.get(b"new").unwrap(), b"value");
    {
        let mut abandoned = map.batch();
        abandoned.remove(b"new");
        abandoned.insert(b"discarded", b"value");
    }
    map.batch().commit_sync().unwrap();
    assert_eq!(map.get(b"new").unwrap(), b"value");
    assert_eq!(map.get(b"discarded"), None);
}

#[test]
fn child() {
    let Ok(root) = env::var("VSDB_SYNC_BATCH_PROBE_ROOT") else {
        return;
    };
    let mode = env::var("VSDB_SYNC_BATCH_PROBE_MODE").unwrap();
    if mode == "read" {
        vsdb_configure(VsdbOptions::read_only(root)).unwrap();
        let id: InstanceId = env::var("VSDB_SYNC_BATCH_PROBE_ID")
            .unwrap()
            .parse()
            .unwrap();
        let mut map = MapxRaw::from_meta(id).unwrap();
        assert_eq!(map.get(b"receipt").unwrap(), b"accepted");
        assert_eq!(map.get(b"quota").unwrap(), b"1");
        assert!(matches!(
            map.batch().commit_sync(),
            Err(VsdbError::ReadOnly { .. })
        ));
        let mut batch = map.batch();
        batch.remove(b"receipt");
        assert!(matches!(
            batch.commit_sync(),
            Err(VsdbError::ReadOnly { .. })
        ));
        return;
    }
    vsdb_configure(VsdbOptions::new(root)).unwrap();
    let ns = Namespace::create().unwrap();
    let mut map = MapxRaw::new_in(&ns);
    let id = map.save_meta().unwrap();
    let mut batch = map.batch();
    batch.insert(b"receipt", b"accepted");
    batch.insert(b"quota", b"1");
    batch.commit_sync().unwrap();
    println!("ACK {id}");
    std::io::stdout().flush().unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn acknowledged_batch_survives_process_kill() {
    let root =
        env::temp_dir().join(format!("vsdb-sync-batch-{}", rand::random::<u128>()));
    let mut writer = Command::new(env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture"])
        .env("VSDB_SYNC_BATCH_PROBE_ROOT", &root)
        .env("VSDB_SYNC_BATCH_PROBE_MODE", "write")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let id = BufReader::new(writer.stdout.take().unwrap())
        .lines()
        .map(|line| line.unwrap())
        .find_map(|line| line.strip_prefix("ACK ").map(str::to_owned))
        .expect("writer must acknowledge its synchronous batch");
    writer.kill().unwrap();
    writer.wait().unwrap();
    let read = Command::new(env::current_exe().unwrap())
        .args(["--exact", "child", "--nocapture"])
        .env("VSDB_SYNC_BATCH_PROBE_ROOT", &root)
        .env("VSDB_SYNC_BATCH_PROBE_MODE", "read")
        .env("VSDB_SYNC_BATCH_PROBE_ID", id)
        .output()
        .unwrap();
    assert!(
        read.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&read.stdout),
        String::from_utf8_lossy(&read.stderr)
    );
    fs::remove_dir_all(root).unwrap();
}
