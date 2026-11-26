//! Write-back for decoded values in ordinary maps, iterators and DAG overlays.
use super::{UnwindMark, ende::ValueEnDe, error::Result};
use std::ops::{Deref, DerefMut};
use vsdb_core::basic::mapx_raw;

pub trait RawValueSlot: DerefMut<Target = Vec<u8>> {
    fn commit(self) -> Result<()>;
}

impl RawValueSlot for mapx_raw::ValueMut<'_> {
    fn commit(self) -> Result<()> {
        self.commit()
    }
}

/// A decoded value held for editing.
///
/// Edits are written back when the guard drops. A panic that starts while
/// the guard is alive discards the edit instead of persisting a half-done
/// value.
#[derive(Debug)]
pub struct ValueGuard<V: ValueEnDe, S: RawValueSlot> {
    value: Option<V>,
    slot: Option<S>,
    dirty: bool,
    vacant: bool,
    #[cfg(debug_assertions)]
    initial: Option<Vec<u8>>,
    unwind: UnwindMark,
}

impl<V: ValueEnDe, S: RawValueSlot> ValueGuard<V, S> {
    pub(crate) fn occupied(slot: S) -> Self {
        Self::try_occupied(slot).expect("vsdb: decode entry")
    }
    pub(crate) fn try_occupied(slot: S) -> Result<Self> {
        let value = V::decode_value(&slot)?;
        Ok(Self::new(slot, value, false))
    }
    pub(crate) fn new(slot: S, value: V, vacant: bool) -> Self {
        Self {
            #[cfg(debug_assertions)]
            initial: (!vacant && !std::thread::panicking())
                .then(|| {
                    let mut bytes = Vec::new();
                    value.encode_value_into(&mut bytes).ok().map(|()| bytes)
                })
                .flatten(),
            value: Some(value),
            slot: Some(slot),
            dirty: false,
            vacant,
            unwind: UnwindMark::new(),
        }
    }
    pub(crate) fn into_parts(mut self) -> (S, V) {
        (self.slot.take().unwrap(), self.value.take().unwrap())
    }

    fn write(&mut self) -> Result<()> {
        // Taking the slot first also suppresses retry on a failed explicit commit.
        let Some(mut slot) = self.slot.take() else {
            return Ok(());
        };
        let value = self.value.as_ref().unwrap();
        if self.dirty || self.vacant {
            let mut bytes = Vec::new();
            value.encode_value_into(&mut bytes)?;
            if self.vacant || bytes != *slot {
                *slot = bytes;
            }
            return slot.commit();
        }
        #[cfg(debug_assertions)]
        if !std::thread::panicking()
            && let Some(initial) = &self.initial
        {
            let mut current = Vec::new();
            let mut second = Vec::new();
            // A codec that assigns a fresh tag on each encode cannot detect
            // interior mutations by byte comparison.
            if value.encode_value_into(&mut current).is_ok()
                && &current != initial
                && value.encode_value_into(&mut second).is_ok()
                && current == second
            {
                debug_assert_eq!(
                    &current, initial,
                    "interior mutation requires a mutable dereference"
                );
            }
        }
        Ok(())
    }
    pub fn commit(mut self) -> Result<()> {
        self.write()
    }
}
impl<V: ValueEnDe, S: RawValueSlot> Drop for ValueGuard<V, S> {
    fn drop(&mut self) {
        if !self.unwind.interrupted() {
            self.write().expect("vsdb: entry write-back failed");
        }
    }
}
impl<V: ValueEnDe, S: RawValueSlot> Deref for ValueGuard<V, S> {
    type Target = V;
    fn deref(&self) -> &V {
        self.value.as_ref().unwrap()
    }
}
impl<V: ValueEnDe, S: RawValueSlot> DerefMut for ValueGuard<V, S> {
    fn deref_mut(&mut self) -> &mut V {
        self.dirty = true;
        self.value.as_mut().unwrap()
    }
}
