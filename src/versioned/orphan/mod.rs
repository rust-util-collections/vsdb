//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

use crate::{
    versioned::mapx_ord_rawkey::MapxOrdRawKeyVs, BranchName, ParentBranchName,
    ValueEnDe, VersionName, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};

/// Used to express some 'non-collection' types,
/// such as any type of integer, an enum value, etc..
///
/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(bound = "")]
pub struct OrphanVs<T>
where
    T: ValueEnDe,
{
    inner: MapxOrdRawKeyVs<T>,
}

impl<T> OrphanVs<T>
where
    T: ValueEnDe,
{
    #[inline(always)]
    pub fn new(v: T) -> Self {
        let hdr = MapxOrdRawKeyVs::new();
        pnk!(hdr.version_create(VersionName(&[])));
        pnk!(hdr.insert_ref(&[], &v));
        Self { inner: hdr }
    }

    #[inline(always)]
    pub fn get_value(&self) -> Option<T> {
        self.inner.get(&[])
    }

    #[inline(always)]
    pub fn set_value(&self, v: T) -> Result<Option<T>> {
        self.set_value_ref(&v).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref(&self, v: &T) -> Result<Option<T>> {
        self.inner.insert_ref(&[], v).c(d!())
    }

    #[inline(always)]
    pub fn get_value_by_branch(&self, branch_name: BranchName) -> Option<T> {
        self.inner.get_by_branch(&[], branch_name)
    }

    #[inline(always)]
    pub fn set_value_by_branch(
        &self,
        v: T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.set_value_ref_by_branch(&v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref_by_branch(
        &self,
        v: &T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.inner.insert_ref_by_branch(&[], v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn get_value_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<T> {
        self.inner
            .get_by_branch_version(&[], branch_name, version_name)
    }
}

impl<T> VsMgmt for OrphanVs<T>
where
    T: ValueEnDe,
{
    crate::impl_vs_methods!();
}
