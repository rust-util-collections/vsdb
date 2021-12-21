//!
//! NOTE: Documents => [MapxRaw](crate::versioned::mapx_raw)
//!

use crate::{
    versioned::mapx_ord_rawkey::MapxOrdRawKeyVs, BranchName, ParentBranchName,
    ValueEnDe, VerChecksum, VersionName,
};
use ruc::*;
use serde::{Deserialize, Serialize};

/// Used to express some 'non-collection' types,
/// such as any type of integer, an enum value, etc..
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
    pub fn new(v: T, version_name: VersionName) -> Self {
        let mut hdr = MapxOrdRawKeyVs::new();
        pnk!(hdr.version_create(version_name));
        pnk!(hdr.insert_ref(&[], &v));
        Self { inner: hdr }
    }

    #[inline(always)]
    pub fn get_value(&self) -> Option<T> {
        self.inner.get(&[])
    }

    #[inline(always)]
    pub fn set_value(&mut self, v: T) -> Result<Option<T>> {
        self.set_value_ref(&v).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref(&mut self, v: &T) -> Result<Option<T>> {
        self.inner.insert_ref(&[], v).c(d!())
    }

    #[inline(always)]
    pub fn get_value_by_branch(&self, branch_name: BranchName) -> Option<T> {
        self.inner.get_by_branch(&[], branch_name)
    }

    #[inline(always)]
    pub fn set_value_by_branch(
        &mut self,
        v: T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.set_value_ref_by_branch(&v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn set_value_ref_by_branch(
        &mut self,
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

    crate::impl_vcs_methods!();
}
