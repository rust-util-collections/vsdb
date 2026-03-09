#[cfg(test)]
mod tests {
    use crate::MptCalc;

    #[test]
    fn test_simple_insert_get() {
        let mut trie = MptCalc::new();

        trie.insert(b"hello", b"world").unwrap();
        assert_eq!(trie.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(trie.get(b"hell").unwrap(), None);

        trie.insert(b"foo", b"bar").unwrap();
        assert_eq!(trie.get(b"foo").unwrap(), Some(b"bar".to_vec()));

        trie.remove(b"hello").unwrap();
        assert_eq!(trie.get(b"hello").unwrap(), None);
        assert_eq!(trie.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    }

    #[test]
    fn test_branch_split() {
        let mut trie = MptCalc::new();

        // "do", "dog" -> Split leaf into Branch
        trie.insert(b"dog", b"puppy").unwrap();
        trie.insert(b"do", b"verb").unwrap();

        assert_eq!(trie.get(b"dog").unwrap(), Some(b"puppy".to_vec()));
        assert_eq!(trie.get(b"do").unwrap(), Some(b"verb".to_vec()));
    }

    #[test]
    fn test_extension_split() {
        let mut trie = MptCalc::new();

        // "abc", "abd" -> Extension "ab", Branch at 'c'/'d'
        trie.insert(b"abc", b"1").unwrap();
        trie.insert(b"abd", b"2").unwrap();

        assert_eq!(trie.get(b"abc").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"abd").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_clone_preserves_state() {
        let mut trie = MptCalc::new();
        trie.insert(b"key1", b"val1").unwrap();
        trie.insert(b"key2", b"val2").unwrap();
        trie.insert(b"key3", b"val3").unwrap();

        // Clone preserves the full in-memory trie.
        let trie2 = trie.clone();
        assert_eq!(trie2.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(trie2.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(trie2.get(b"key3").unwrap(), Some(b"val3".to_vec()));
        assert_eq!(trie2.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_overwrite_value() {
        let mut trie = MptCalc::new();

        trie.insert(b"key", b"val1").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val1".to_vec()));

        trie.insert(b"key", b"val2").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_remove_all_keys() {
        let mut trie = MptCalc::new();

        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();

        trie.remove(b"a").unwrap();
        trie.remove(b"b").unwrap();

        assert_eq!(trie.get(b"a").unwrap(), None);
        assert_eq!(trie.get(b"b").unwrap(), None);

        // Root should be the empty root
        assert_eq!(trie.root_hash().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let mut trie = MptCalc::new();

        trie.insert(b"exists", b"yes").unwrap();
        let root_before = trie.root_hash().unwrap();

        trie.remove(b"does_not_exist").unwrap();
        let root_after = trie.root_hash().unwrap();

        // Root should not change
        assert_eq!(root_before, root_after);
        assert_eq!(trie.get(b"exists").unwrap(), Some(b"yes".to_vec()));
    }

    #[test]
    fn test_batch_update() {
        let mut trie = MptCalc::new();

        trie.batch_update(&[
            (b"k1".as_slice(), Some(b"v1".as_slice())),
            (b"k2".as_slice(), Some(b"v2".as_slice())),
            (b"k3".as_slice(), Some(b"v3".as_slice())),
        ])
        .unwrap();

        assert_eq!(trie.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(trie.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(trie.get(b"k3").unwrap(), Some(b"v3".to_vec()));

        // Batch with mixed insert/remove
        trie.batch_update(&[
            (b"k1".as_slice(), None),
            (b"k4".as_slice(), Some(b"v4".as_slice())),
        ])
        .unwrap();

        assert_eq!(trie.get(b"k1").unwrap(), None);
        assert_eq!(trie.get(b"k4").unwrap(), Some(b"v4".to_vec()));
    }

    #[test]
    fn test_deterministic_root() {
        let mut trie_a = MptCalc::new();
        trie_a.insert(b"x", b"1").unwrap();
        trie_a.insert(b"y", b"2").unwrap();
        trie_a.insert(b"z", b"3").unwrap();

        // Insert in different order, same data
        let mut trie_b = MptCalc::new();
        trie_b.insert(b"z", b"3").unwrap();
        trie_b.insert(b"x", b"1").unwrap();
        trie_b.insert(b"y", b"2").unwrap();

        assert_eq!(trie_a.root_hash().unwrap(), trie_b.root_hash().unwrap());
    }

    #[test]
    fn test_empty_key_and_value() {
        let mut trie = MptCalc::new();

        trie.insert(b"", b"empty_key").unwrap();
        assert_eq!(trie.get(b"").unwrap(), Some(b"empty_key".to_vec()));

        trie.insert(b"nonempty", b"").unwrap();
        assert_eq!(trie.get(b"nonempty").unwrap(), Some(b"".to_vec()));
    }

    #[test]
    fn test_many_keys() {
        let mut trie = MptCalc::new();

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            let val = (i * 2).to_be_bytes();
            trie.insert(&key, &val).unwrap();
        }

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            let val = (i * 2).to_be_bytes();
            assert_eq!(trie.get(&key).unwrap(), Some(val.to_vec()));
        }

        // Remove half
        for i in (0u32..100).step_by(2) {
            let key = i.to_be_bytes();
            trie.remove(&key).unwrap();
        }

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            if i % 2 == 0 {
                assert_eq!(trie.get(&key).unwrap(), None);
            } else {
                let val = (i * 2).to_be_bytes();
                assert_eq!(trie.get(&key).unwrap(), Some(val.to_vec()));
            }
        }
    }

    #[test]
    fn test_from_entries() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        let trie = MptCalc::from_entries(entries).unwrap();
        assert_eq!(trie.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(trie.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_from_entries_matches_incremental() {
        let entries = vec![
            (b"x".to_vec(), b"1".to_vec()),
            (b"y".to_vec(), b"2".to_vec()),
            (b"z".to_vec(), b"3".to_vec()),
        ];
        let mut from_entries = MptCalc::from_entries(entries).unwrap();

        let mut incremental = MptCalc::new();
        incremental.insert(b"x", b"1").unwrap();
        incremental.insert(b"y", b"2").unwrap();
        incremental.insert(b"z", b"3").unwrap();

        assert_eq!(
            from_entries.root_hash().unwrap(),
            incremental.root_hash().unwrap()
        );
    }

    #[test]
    fn test_cache_save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("mpt_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache_path = dir.join("trie.cache");

        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        trie.insert(b"foo", b"bar").unwrap();
        trie.insert(b"key", b"value").unwrap();
        let root = trie.root_hash().unwrap();

        trie.save_cache(&cache_path, 42).unwrap();

        let (loaded, sync_tag, loaded_root) = MptCalc::load_cache(&cache_path).unwrap();
        assert_eq!(sync_tag, 42);
        assert_eq!(loaded_root, root);
        assert_eq!(loaded.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(loaded.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert_eq!(loaded.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(loaded.get(b"missing").unwrap(), None);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_cache_incremental_after_load() {
        let dir = std::env::temp_dir().join(format!("mpt_test_inc_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache_path = dir.join("trie.cache");

        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.save_cache(&cache_path, 1).unwrap();

        // Load and apply incremental changes.
        let (mut loaded, _, _) = MptCalc::load_cache(&cache_path).unwrap();
        loaded.insert(b"c", b"3").unwrap();
        loaded.remove(b"a").unwrap();

        assert_eq!(loaded.get(b"a").unwrap(), None);
        assert_eq!(loaded.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(loaded.get(b"c").unwrap(), Some(b"3".to_vec()));

        // Root hash should match a fresh build with the same data.
        let mut fresh = MptCalc::new();
        fresh.insert(b"b", b"2").unwrap();
        fresh.insert(b"c", b"3").unwrap();
        assert_eq!(loaded.root_hash().unwrap(), fresh.root_hash().unwrap());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_cache_empty_trie() {
        let dir = std::env::temp_dir().join(format!("mpt_test_empty_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache_path = dir.join("trie.cache");

        let mut trie = MptCalc::new();
        trie.save_cache(&cache_path, 0).unwrap();

        let (loaded, sync_tag, root_hash) = MptCalc::load_cache(&cache_path).unwrap();
        assert_eq!(sync_tag, 0);
        assert_eq!(root_hash, vec![0u8; 32]);
        assert_eq!(loaded.get(b"anything").unwrap(), None);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_cache_many_keys() {
        let dir = std::env::temp_dir().join(format!("mpt_test_many_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache_path = dir.join("trie.cache");

        let mut trie = MptCalc::new();
        for i in 0u32..200 {
            trie.insert(&i.to_be_bytes(), &(i * 3).to_be_bytes()).unwrap();
        }
        let root = trie.root_hash().unwrap();
        trie.save_cache(&cache_path, 99).unwrap();

        let (mut loaded, _, loaded_root) = MptCalc::load_cache(&cache_path).unwrap();
        assert_eq!(loaded_root, root);

        for i in 0u32..200 {
            assert_eq!(
                loaded.get(&i.to_be_bytes()).unwrap(),
                Some((i * 3).to_be_bytes().to_vec())
            );
        }
        assert_eq!(loaded.root_hash().unwrap(), root);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
