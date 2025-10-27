#[cfg(test)]
mod tests {
    use crate::MptStore;

    #[test]
    fn test_simple_insert_get() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

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
        let store = MptStore::new();
        let mut trie = store.trie_init();

        // "do", "dog" -> Split leaf into Branch
        trie.insert(b"dog", b"puppy").unwrap();
        trie.insert(b"do", b"verb").unwrap();

        assert_eq!(trie.get(b"dog").unwrap(), Some(b"puppy".to_vec()));
        assert_eq!(trie.get(b"do").unwrap(), Some(b"verb".to_vec()));
    }

    #[test]
    fn test_extension_split() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

        // "abc", "abd" -> Extension "ab", Branch at 'c'/'d'
        trie.insert(b"abc", b"1").unwrap();
        trie.insert(b"abd", b"2").unwrap();

        assert_eq!(trie.get(b"abc").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"abd").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_persistence_reload() {
        let store = MptStore::new();
        let root;
        {
            let mut trie = store.trie_init();
            trie.insert(b"key1", b"val1").unwrap();
            println!("root1: {:?}", trie.root());
            trie.insert(b"key2", b"val2").unwrap();
            println!("root2: {:?}", trie.root());
            trie.insert(b"key3", b"val3").unwrap();
            println!("root3: {:?}", trie.root());
            root = trie.root();
        }

        // Reload from root hash
        let trie = store.trie_load(&root);
        assert_eq!(trie.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(trie.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(trie.get(b"key3").unwrap(), Some(b"val3".to_vec()));
        assert_eq!(trie.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_overwrite_value() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

        trie.insert(b"key", b"val1").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val1".to_vec()));

        trie.insert(b"key", b"val2").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_remove_all_keys() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();

        trie.remove(b"a").unwrap();
        trie.remove(b"b").unwrap();

        assert_eq!(trie.get(b"a").unwrap(), None);
        assert_eq!(trie.get(b"b").unwrap(), None);

        // Root should be the empty root
        assert_eq!(trie.root(), vec![0u8; 32]);
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

        trie.insert(b"exists", b"yes").unwrap();
        let root_before = trie.root();

        trie.remove(b"does_not_exist").unwrap();
        let root_after = trie.root();

        // Root should not change
        assert_eq!(root_before, root_after);
        assert_eq!(trie.get(b"exists").unwrap(), Some(b"yes".to_vec()));
    }

    #[test]
    fn test_batch_update() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

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
        let store = MptStore::new();

        // Insert in order A
        let mut trie_a = store.trie_init();
        trie_a.insert(b"x", b"1").unwrap();
        trie_a.insert(b"y", b"2").unwrap();
        trie_a.insert(b"z", b"3").unwrap();

        // Insert in order B (different order, same data)
        // Note: because each insert commits, intermediate roots differ,
        // but final root with same data should be the same.
        let mut trie_b = store.trie_init();
        trie_b.insert(b"z", b"3").unwrap();
        trie_b.insert(b"x", b"1").unwrap();
        trie_b.insert(b"y", b"2").unwrap();

        assert_eq!(trie_a.root(), trie_b.root());
    }

    #[test]
    fn test_empty_key_and_value() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

        trie.insert(b"", b"empty_key").unwrap();
        assert_eq!(trie.get(b"").unwrap(), Some(b"empty_key".to_vec()));

        trie.insert(b"nonempty", b"").unwrap();
        assert_eq!(trie.get(b"nonempty").unwrap(), Some(b"".to_vec()));
    }

    #[test]
    fn test_many_keys() {
        let store = MptStore::new();
        let mut trie = store.trie_init();

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
}
