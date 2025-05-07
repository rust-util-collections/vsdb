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
}
