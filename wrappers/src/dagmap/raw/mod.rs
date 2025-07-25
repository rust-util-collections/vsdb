//!
//! A raw, disk-based, directed acyclic graph (DAG) map.
//!
//! `DagMapRaw` provides a map-like interface where each instance can have a parent
//! and multiple children, forming a directed acyclic graph. This is useful for
//! representing versioned or hierarchical data.
//!
//! # Examples
//!
//! ```
//! use vsdb::{DagMapRaw, Orphan};
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut parent = Orphan::new(None);
//! let mut dag = DagMapRaw::new(&mut parent).unwrap();
//!
//! // Insert a value
//! dag.insert(&[1], &[10]);
//! assert_eq!(dag.get(&[1]), Some(vec![10]));
//!
//! // Create a child
//! let mut child = DagMapRaw::new(&mut Orphan::new(Some(dag))).unwrap();
//! assert_eq!(child.get(&[1]), Some(vec![10]));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{DagMapId, MapxOrdRawKey, MapxRaw, Orphan};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
};
use vsdb_core::{basic::mapx_raw, common::RawBytes};

type DagHead = DagMapRaw;

/// A raw, disk-based, directed acyclic graph (DAG) map.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DagMapRaw {
    data: MapxRaw,

    parent: Orphan<Option<DagMapRaw>>,

    // child id --> child instance
    children: MapxOrdRawKey<DagMapRaw>,
}

impl DagMapRaw {
    /// Creates a new `DagMapRaw`.
    pub fn new(parent: &mut Orphan<Option<Self>>) -> Result<Self> {
        let r = Self {
            parent: unsafe { parent.shadow() },
            ..Default::default()
        };

        if let Some(p) = parent.get_mut().as_mut() {
            let child_id = super::gen_dag_map_id_num().to_be_bytes();
            if p.children.insert(child_id, &r).is_some() {
                return Err(eg!("Error! Child ID exist!"));
            }
        }

        Ok(r)
    }

    /// Creates a "shadow" copy of the `DagMapRaw` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                data: self.data.shadow(),
                parent: self.parent.shadow(),
                children: self.children.shadow(),
            }
        }
    }

    /// Checks if the DAG map is dead (i.e., has no data, parent, or children).
    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.data.is_empty() && self.parent.get_value().is_none() && self.no_children()
    }

    /// Checks if the DAG map has no children.
    #[inline(always)]
    pub fn no_children(&self) -> bool {
        self.children.is_empty()
    }

    /// Retrieves a value from the DAG map, traversing up to the parent if necessary.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        let key = key.as_ref();

        let mut hdr = self;
        let mut hdr_owned;

        loop {
            if let Some(v) = hdr.data.get(key) {
                return alt!(v.is_empty(), None, Some(v));
            }
            match hdr.parent.get_value() {
                Some(p) => {
                    hdr_owned = p;
                    hdr = &hdr_owned;
                }
                _ => {
                    return None;
                }
            }
        }
    }

    /// Retrieves a mutable reference to a value in the DAG map.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.data.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: inner.clone(),
            inner,
        })
    }

    /// Inserts a key-value pair into the DAG map.
    #[inline(always)]
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Option<RawBytes> {
        self.data.insert(key.as_ref(), value)
    }

    /// Removes a key-value pair from the DAG map.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<RawBytes> {
        self.data.insert(key.as_ref(), [])
    }

    /// Prunes the DAG, merging all nodes in the mainline into the genesis node.
    ///
    /// Returns the new head of the mainline.
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead> {
        self.prune_mainline().c(d!())
    }

    // Return the new head of mainline
    fn prune_mainline(mut self) -> Result<DagHead> {
        let p = match self.parent.get_value() {
            Some(p) => p,
            _ => {
                return Ok(self);
            }
        };

        let mut linebuf = vec![p];
        while let Some(p) = linebuf.last().unwrap().parent.get_value() {
            linebuf.push(p);
        }

        let mid = linebuf.len() - 1;
        let (others, genesis) = linebuf.split_at_mut(mid);

        for i in others.iter().rev() {
            for (k, v) in i.data.iter() {
                genesis[0].data.insert(k, v);
            }
        }

        for (k, v) in self.data.iter() {
            genesis[0].data.insert(k, v);
        }

        let mut exclude_targets = vec![];
        for (id, mut child) in self.children.iter_mut() {
            *child.parent.get_mut() = Some(unsafe { genesis[0].shadow() });
            genesis[0].children.insert(&id, &child);
            exclude_targets.push(id);
        }

        // clean up
        *self.parent.get_mut() = None;
        self.data.clear();
        self.children.clear(); // disconnect from the mainline

        genesis[0].prune_children_exclude(&exclude_targets);

        // genesis[0]
        Ok(linebuf.pop().unwrap())
    }

    /// Prunes children that are in the `include_targets` list.
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(include_targets, false);
    }

    /// Prunes children that are not in the `exclude_targets` list.
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<DagMapId>]) {
        self.prune_children(exclude_targets, true);
    }

    fn prune_children(&mut self, targets: &[impl AsRef<DagMapId>], exclude_mode: bool) {
        let targets = targets.iter().map(|i| i.as_ref()).collect::<HashSet<_>>();

        let dropped_children = if exclude_mode {
            self.children
                .iter()
                .filter(|(id, _)| !targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        } else {
            self.children
                .iter()
                .filter(|(id, _)| targets.contains(&id.as_slice()))
                .collect::<Vec<_>>()
        };

        for (id, _) in dropped_children.iter() {
            self.children.remove(id);
        }

        for (_, mut child) in dropped_children.into_iter() {
            child.destroy();
        }
    }

    /// Destroys the DAG map and all its children, clearing all data.
    #[inline(always)]
    pub fn destroy(&mut self) {
        *self.parent.get_mut() = None;
        self.data.clear();

        let mut children = self.children.iter().map(|(_, c)| c).collect::<Vec<_>>();
        self.children.clear(); // optimize for recursive ops

        for c in children.iter_mut() {
            c.destroy();
        }
    }

    /// Checks if this `DagMapRaw` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.data.is_the_same_instance(&other_hdr.data)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `DagMapRaw`.
#[derive(Debug)]
pub struct ValueMut<'a> {
    value: RawBytes,
    inner: mapx_raw::ValueMut<'a>,
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        self.inner.clone_from(&self.value);
    }
}

impl Deref for ValueMut<'_> {
    type Target = RawBytes;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
