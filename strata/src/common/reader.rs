//! Typed read handles share storage identity and retain the owning namespace.
use super::{
    SingleMap,
    ende::{
        KeyEnDe, KeyEnDeOrdered, KeyLookup, OrderedKeyLookup, OrderedRangeBounds,
        ValueEnDe,
    },
};
use crate::{Mapx, MapxOrd, MapxOrdRawKey, Orphan};
use std::marker::PhantomData;
use vsdb_core::{InstanceId, MapxRawReadView, MapxRawReader, Namespace, RawRangeBounds};

/// Immutable configuration retained alongside a collection's raw reader.
pub trait ReadableMap {
    type Config: Clone;
}

/// An independently owned reader. Cloning retains the same map and namespace.
pub struct MapReader<M: ReadableMap> {
    pub(crate) raw: MapxRawReader,
    pub(crate) config: M::Config,
}
impl<M: ReadableMap> MapReader<M> {
    pub(crate) fn new(raw: MapxRawReader, config: M::Config) -> Self {
        Self { raw, config }
    }
    pub fn namespace(&self) -> Namespace {
        self.raw.namespace()
    }
    pub fn instance_id(&self) -> InstanceId {
        self.raw.instance_id()
    }
}
impl<M: ReadableMap> Clone for MapReader<M> {
    fn clone(&self) -> Self {
        Self::new(self.raw.clone(), self.config.clone())
    }
}

macro_rules! basic_view {
    ($map:ident<$($param:ident: $bound:ident),+>, $view:ident) => {
        impl<$($param),+> ReadableMap for $map<$($param),+> {
            type Config = ();
        }

        impl<$($param: $bound),+> $map<$($param),+> {
            /// Shares this map's identity with a read-only handle.
            pub fn reader(&self) -> MapReader<Self> {
                MapReader::new(self.raw().reader(), ())
            }

            /// Captures the current state for a sequence of queries.
            pub fn read_view(&self) -> $view<'_, $($param),+> {
                $view { raw: self.raw().read_view(), types: PhantomData }
            }
        }

        /// A set of queries sharing one engine snapshot.
        pub struct $view<'a, $($param),+> {
            raw: MapxRawReadView<'a>,
            types: PhantomData<fn() -> $map<$($param),+>>,
        }

        impl<$($param: $bound),+> MapReader<$map<$($param),+>> {
            pub fn read_view(&self) -> $view<'_, $($param),+> {
                $view { raw: self.raw.read_view(), types: PhantomData }
            }
        }
    };
}
basic_view!(Mapx<K: KeyEnDe, V: ValueEnDe>, MapxReadView);
basic_view!(MapxOrd<K: KeyEnDeOrdered, V: ValueEnDe>, MapxOrdReadView);
basic_view!(MapxOrdRawKey<V: ValueEnDe>, MapxOrdRawKeyReadView);

fn decode<V: ValueEnDe>(bytes: Vec<u8>) -> V {
    V::decode_value(&bytes).expect("vsdb: decode value")
}
impl<K: KeyEnDe, V: ValueEnDe> MapxReadView<'_, K, V> {
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: KeyLookup<Q>,
    {
        K::with_lookup(key, |bytes| {
            self.raw
                .try_get_with(bytes, V::decode_value)
                .expect("vsdb: read and decode value")
        })
        .flatten()
    }
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: KeyLookup<Q>,
    {
        K::with_lookup(key, |bytes| self.raw.contains_key(bytes)).unwrap_or(false)
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.raw
            .iter()
            .map(|(k, v)| (K::decode_key(&k).expect("vsdb: decode key"), decode(v)))
    }
}
impl<K: KeyEnDeOrdered, V: ValueEnDe> MapxOrdReadView<'_, K, V> {
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: OrderedKeyLookup<Q>,
    {
        K::with_ordered_lookup(key, |bytes| {
            self.raw
                .try_get_with(bytes, V::decode_value)
                .expect("vsdb: read and decode value")
        })
        .flatten()
    }
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: OrderedKeyLookup<Q>,
    {
        K::with_ordered_lookup(key, |bytes| self.raw.contains_key(bytes))
            .unwrap_or(false)
    }
    pub fn range(
        &self,
        bounds: impl OrderedRangeBounds<K>,
    ) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.raw.range(bounds.ordered_bounds()).map(|(k, v)| {
            (
                K::from_bytes(k).expect("vsdb: decode ordered key"),
                decode(v),
            )
        })
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (K, V)> + '_ {
        self.range(..)
    }
}
impl<V: ValueEnDe> MapxOrdRawKeyReadView<'_, V> {
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.raw
            .try_get_with(key, V::decode_value)
            .expect("vsdb: read and decode value")
    }
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.raw.contains_key(key)
    }
    pub fn range(
        &self,
        bounds: impl RawRangeBounds,
    ) -> impl DoubleEndedIterator<Item = (Vec<u8>, V)> + '_ {
        self.raw.range(bounds).map(|(k, v)| (k, decode(v)))
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (Vec<u8>, V)> + '_ {
        self.range(..)
    }
}

pub type OrphanReader<T> = MapReader<Orphan<T>>;
impl<T> ReadableMap for Orphan<T> {
    type Config = ();
}
impl<T: ValueEnDe> Orphan<T> {
    pub fn reader(&self) -> OrphanReader<T> {
        MapReader::new(self.raw().reader(), ())
    }
}
impl<T: ValueEnDe> OrphanReader<T> {
    pub fn get(&self) -> T {
        self.raw
            .try_get_with([], T::decode_value)
            .expect("vsdb: read and decode orphan")
            .expect("vsdb: missing orphan value")
    }
}
