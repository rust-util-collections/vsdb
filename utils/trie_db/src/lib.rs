#![deny(warnings)]
#![allow(clippy::new_without_default)]

pub use vsdb::{RawBytes, RawKey, RawValue, ValueEnDe};

use reference_trie::{
    ExtensionLayout as L, RefTrieDB as TrieDB, RefTrieDBBuilder as TrieDBBuilder,
    RefTrieDBMut as TrieDBMut, RefTrieDBMutBuilder as TrieDBMutBuilder,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use trie_db::{
    CError, DBValue, HashDB, Hasher as _, Trie, TrieHash, TrieItem, TrieIterator, TrieKeyItem,
    TrieMut,
};
use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
use vsdb_hash_db::{KeccakHasher as H, TrieBackend};

pub type TrieRoot = TrieHash<L>;

pub type TrieIter<'a> = Box<dyn TrieIterator<L, Item = TrieItem<TrieHash<L>, CError<L>>> + 'a>;
pub type TrieKeyIter<'a> =
    Box<dyn TrieIterator<L, Item = TrieKeyItem<TrieHash<L>, CError<L>>> + 'a>;

#[derive(Deserialize, Serialize)]
pub struct MptStore {
    // backend key ==> backend instance
    //
    // the backend key
    // - for the world state MPT, it is `[0]`
    // - for the storage MPT, it is the bytes of a H160 address
    meta: MapxOrdRawKey<TrieBackend>,
}

impl MptStore {
    pub fn new() -> Self {
        Self {
            meta: MapxOrdRawKey::new(),
        }
    }

    pub fn trie_remove(&self, backend_key: &[u8]) {
        self.remove_backend(backend_key);
    }

    pub fn trie_create(&self, backend_key: &[u8], reset: bool) -> Result<MptOnce> {
        let backend = MptStore::new_backend();
        self.put_backend(backend_key, &backend, reset).c(d!())?;

        MptOnce::create_with_backend(backend).c(d!())
    }

    pub fn trie_restore(&self, backend_key: &[u8], root: TrieRoot) -> Result<MptOnce> {
        self.get_backend(backend_key)
            .c(d!("backend not found"))
            .and_then(|backend| MptOnce::restore(backend, root).c(d!()))
    }

    fn get_backend(&self, backend_key: &[u8]) -> Option<TrieBackend> {
        self.meta.get(backend_key)
    }

    fn put_backend(&self, backend_key: &[u8], backend: &TrieBackend, reset: bool) -> Result<()> {
        let mut hdr = unsafe { self.meta.shadow() };

        if reset {
            hdr.remove(backend_key);
        } else if hdr.contains_key(backend_key) {
            return Err(eg!("backend key already exists"));
        }

        hdr.insert(backend_key, backend);

        Ok(())
    }

    fn remove_backend(&self, backend_key: &[u8]) {
        unsafe { self.meta.shadow() }.remove(backend_key);
    }

    fn new_backend() -> TrieBackend {
        TrieBackend::new()
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `Drop::drop` can be executed in the correct order,
// so that UB will not occur
/// An owned MPT instance
pub struct MptOnce {
    mpt: MptMut<'static>,
    root: TrieRoot,

    // self-reference
    #[allow(dead_code)]
    backend: Box<TrieBackend>,
}

impl MptOnce {
    pub fn create() -> Result<Self> {
        Self::create_with_backend(TrieBackend::new()).c(d!())
    }

    pub fn create_with_backend(backend: TrieBackend) -> Result<Self> {
        let backend = Box::into_raw(Box::new(backend));
        let mut mpt = MptMut::new(unsafe { &mut *backend });
        let root = mpt.commit();
        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(backend) },
        })
    }

    pub fn restore(backend: TrieBackend, root: TrieRoot) -> Result<Self> {
        let backend = Box::into_raw(Box::new(backend));
        let mpt = MptMut::from_existing(unsafe { &mut *backend }, root);
        Ok(Self {
            mpt,
            root,
            backend: unsafe { Box::from_raw(backend) },
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.mpt.get(key).c(d!())
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.mpt.contains(key).c(d!())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.mpt.insert(key, value).c(d!())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.mpt.remove(key).c(d!()).map(|_| ())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.mpt.clear().c(d!())
    }

    pub fn is_empty(&self) -> bool {
        self.mpt.is_empty()
    }

    pub fn commit(&mut self) -> TrieRoot {
        self.root = self.mpt.commit();
        self.root
    }

    pub fn root(&self) -> TrieRoot {
        self.root
    }

    pub fn ro_handle(&self, root: TrieRoot) -> MptRo {
        MptRo::from_existing(&self.backend, root)
    }
}

impl ValueEnDe for MptOnce {
    fn try_encode(&self) -> Result<RawBytes> {
        Ok(self.encode())
    }

    fn encode(&self) -> RawBytes {
        let mut buf = self.root.to_vec();
        buf.append(&mut self.backend.encode());
        buf
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        alt!(H::LENGTH > bytes.len(), return Err(eg!("Invalid length")));

        let mut root = [0; H::LENGTH];
        root.copy_from_slice(&bytes[..H::LENGTH]);

        let backend = TrieBackend::decode(&bytes[H::LENGTH..]).c(d!())?;

        Self::restore(backend, root).c(d!())
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
/// A mutable MPT instance
pub struct MptMut<'a> {
    trie: TrieDBMut<'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptMut<'a> {
    // keep private !!
    pub fn new(backend: &'a mut TrieBackend) -> Self {
        // The buf will be rewrited when building the target `Trie`,
        // so its original contents can be arbitrary values.
        let root_buf = Default::default();

        let meta = MptMeta::new(root_buf);

        let trie = TrieDBMutBuilder::new(backend, unsafe { &mut *meta.root }).build();

        Self { trie, meta }
    }

    pub fn from_existing(backend: &'a mut TrieBackend, root: TrieRoot) -> Self {
        let meta = MptMeta::new(root);

        let trie = TrieDBMutBuilder::from_existing(backend, unsafe { &mut *meta.root }).build();

        Self { trie, meta }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.trie.insert(key, value).c(d!()).map(|_| ())
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.trie.remove(key).c(d!()).map(|_| ())
    }

    pub fn clear(&mut self) -> Result<()> {
        let root = self.commit();
        let keys = self.ro_handle(root).key_iter().collect::<Vec<_>>();
        for k in keys.iter().map(|k| k.as_ref().unwrap()) {
            self.remove(k).c(d!())?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    pub fn commit(&mut self) -> TrieRoot {
        *self.trie.root()
    }

    pub fn ro_handle(&self, root: TrieRoot) -> MptRo {
        MptRo::from_existing_dyn(self.trie.db(), root)
    }
}

//
// # NOTE
//
// The referenced field **MUST** be placed after the field that references it,
// this is to ensure that the `drop`s can be executed in the correct order,
// so that UB will not occur
/// A readonly MPT instance
pub struct MptRo<'a> {
    trie: TrieDB<'a, 'a>,

    // self-reference
    #[allow(dead_code)]
    meta: MptMeta,
}

impl<'a> MptRo<'a> {
    pub fn from_existing(backend: &'a TrieBackend, root: TrieRoot) -> Self {
        let meta = MptMeta::new(root);

        let trie = TrieDBBuilder::new(backend, unsafe { &*meta.root }).build();

        Self { trie, meta }
    }

    pub fn from_existing_dyn(backend: &dyn HashDB<H, DBValue>, root: TrieRoot) -> Self {
        let backend = &backend as *const &dyn HashDB<H, DBValue>;
        let backend = backend.cast::<&TrieBackend>();
        let backend = unsafe { *backend };
        MptRo::from_existing(backend, root)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.trie.get(key).c(d!())
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.trie.contains(key).c(d!())
    }

    pub fn iter(&self) -> TrieIter<'_> {
        pnk!(self.trie.iter())
    }

    pub fn key_iter(&self) -> TrieKeyIter<'_> {
        pnk!(self.trie.key_iter())
    }

    pub fn root(&mut self) -> TrieRoot {
        *self.trie.root()
    }
}

struct MptMeta {
    // self-reference
    #[allow(dead_code)]
    root: *mut TrieRoot,
}

impl MptMeta {
    fn new(root: TrieRoot) -> Self {
        Self {
            root: Box::into_raw(Box::new(root)),
        }
    }
}

impl Drop for MptMeta {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.root));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn trie_db_encode_decode() {
        let mut hdr = pnk!(MptOnce::create());

        pnk!(hdr.insert(b"key", b"value"));
        assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());

        let root = hdr.commit();
        assert_eq!(root, hdr.root());

        let hdr_encoded = hdr.encode();
        drop(hdr);

        let hdr = pnk!(MptOnce::decode(&hdr_encoded));
        assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());
    }

    #[test]
    fn trie_db_iter() {
        let s = MptStore::new();
        let mut hdr = pnk!(s.trie_create(b"backend_key", false));

        {
            let samples = (0u8..200).map(|i| ([i], [i])).collect::<Vec<_>>();
            samples.iter().for_each(|(k, v)| {
                pnk!(hdr.insert(k, v));
            });

            let root = hdr.commit();

            let ro_hdr = hdr.ro_handle(root);
            let bt = ro_hdr
                .iter()
                .map(|i| i.unwrap())
                .collect::<BTreeMap<_, _>>();

            bt.iter().enumerate().for_each(|(i, (k, v))| {
                assert_eq!(&[i as u8], k.as_slice());
                assert_eq!(k, v);
            });

            let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
            assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());
        }

        {
            let samples = (0u8..200).map(|i| ([i], [i + 1])).collect::<Vec<_>>();
            samples.iter().for_each(|(k, v)| {
                pnk!(hdr.insert(k, v));
            });

            let root = hdr.commit();

            let ro_hdr = hdr.ro_handle(root);
            let bt = ro_hdr
                .iter()
                .map(|i| i.unwrap())
                .collect::<BTreeMap<_, _>>();

            bt.iter().enumerate().for_each(|(i, (k, v))| {
                assert_eq!(&[i as u8], k.as_slice());
                assert_eq!(&[k[0] + 1], v.as_slice());
            });

            let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
            assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());
        }

        assert!(!hdr.is_empty());
        hdr.clear().unwrap();
        assert!(hdr.is_empty());
    }
}
