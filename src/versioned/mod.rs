//!
//! Versioned functions.
//!

pub mod mapx;
pub mod mapx_ord;
pub mod mapx_ord_rawkey;
pub mod mapx_raw;
pub mod orphan;
pub mod vecx;

#[cfg(feature = "merkle")]
use crate::merkle::{MerkleTree, MerkleTreeStore, Proof, ProofEntry};
use crate::{
    basic::{
        mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
        mapx_ord_rawvalue::MapxOrdRawValue, mapx_raw::MapxRaw, orphan::Orphan,
        vecx::Vecx, vecx_raw::VecxRaw,
    },
    BranchName, ParentBranchName, VersionName,
};
use ruc::*;
use std::{
    collections::{
        BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque,
    },
    marker::PhantomData,
    sync::atomic::{
        AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicU16, AtomicU32,
        AtomicU64, AtomicU8,
    },
};

/// Methods collection of version management.
pub trait VsMgmt {
    /// Create a new version on the default branch.
    fn version_create(&self, version_name: VersionName) -> Result<()>;

    /// Create a new version on a specified branch,
    /// NOTE: the branch must has been created.
    fn version_create_by_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> Result<()>;

    /// Check if a verison exists on default branch.
    fn version_exists(&self, version_name: VersionName) -> bool;

    /// Check if a version exists on a specified branch(include its parents).
    fn version_exists_on_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> bool;

    /// Remove the newest version on the default branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn version_pop(&self) -> Result<()>;

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()>;

    /// Merge all changes made by new versions after the base version into the base version.
    ///
    /// # Safety
    ///
    /// It's the caller's duty to ensure that
    /// the `base_version` was created directly by the `branch_id`,
    /// or the data records of other branches may be corrupted.
    unsafe fn version_rebase(&self, base_version: VersionName) -> Result<()>;

    /// Merge all changes made by new versions after the base version into the base version.
    ///
    /// # Safety
    ///
    /// It's the caller's duty to ensure that
    /// the `base_version` was created directly by the `branch_id`,
    /// or the data records of other branches may be corrupted.
    unsafe fn version_rebase_by_branch(
        &self,
        base_version: VersionName,
        branch_name: BranchName,
    ) -> Result<()>;

    /// Create a new branch based on the head of the default branch.
    fn branch_create(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Result<()>;

    /// Create a new branch based on the head of a specified branch.
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
    ) -> Result<()>;

    /// Create a new branch based on a specified version of a specified branch.
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()>;

    /// Check if a branch exists or not.
    fn branch_exists(&self, branch_name: BranchName) -> bool;

    /// Check if a branch exists and has versions on it.
    fn branch_has_versions(&self, branch_name: BranchName) -> bool;

    /// Remove a branch, remove all changes directly made by this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_remove(&self, branch_name: BranchName) -> Result<()>;

    /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_truncate(&self, branch_name: BranchName) -> Result<()>;

    /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_truncate_to(
        &self,
        branch_name: BranchName,
        last_version_name: VersionName,
    ) -> Result<()>;

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_pop_version(&self, branch_name: BranchName) -> Result<()>;

    /// Merge a branch into another.
    fn branch_merge_to(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()>;

    /// Merge a branch into another,
    /// even if new different versions have been created on the target branch.
    ///
    /// # Safety
    ///
    /// If new different versions have been created on the target branch,
    /// the data records referenced by other branches may be corrupted.
    unsafe fn branch_merge_to_force(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()>;

    /// Make a branch to be default,
    /// all default operations will be applied to it.
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()>;

    /// Clean outdated versions out of the default branch.
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()>;
}

#[macro_export(super)]
macro_rules! impl_vs_methods {
    () => {
        /// Create a new version on the default branch.
        #[inline(always)]
        fn version_create(&self, version_name: VersionName) -> Result<()> {
            self.inner.version_create(version_name).c(d!())
        }

        /// Create a new version on a specified branch,
        /// NOTE: the branch must has been created.
        #[inline(always)]
        fn version_create_by_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> Result<()> {
            self.inner
                .version_create_by_branch(version_name, branch_name)
                .c(d!())
        }

        /// Check if a verison exists on default branch.
        #[inline(always)]
        fn version_exists(&self, version_name: VersionName) -> bool {
            self.inner.version_exists(version_name)
        }

        /// Check if a version exists on a specified branch(include its parents).
        #[inline(always)]
        fn version_exists_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            self.inner
                .version_exists_on_branch(version_name, branch_name)
        }

        /// Remove the newest version on the default branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn version_pop(&self) -> Result<()> {
            self.inner.version_pop().c(d!())
        }

        /// Remove the newest version on a specified branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()> {
            self.inner.version_pop_by_branch(branch_name).c(d!())
        }

        /// Merge all changes made by new versions after the base version into the base version.
        ///
        /// # Safety
        ///
        /// It's the caller's duty to ensure that
        /// the `base_version` was created directly by the `branch_id`,
        /// or the data records of other branches may be corrupted.
        #[inline(always)]
        unsafe fn version_rebase(&self, base_version: VersionName) -> Result<()> {
            self.inner.version_rebase(base_version).c(d!())
        }

        /// Merge all changes made by new versions after the base version into the base version.
        ///
        /// # Safety
        ///
        /// It's the caller's duty to ensure that
        /// the `base_version` was created directly by the `branch_id`,
        /// or the data records of other branches may be corrupted.
        #[inline(always)]
        unsafe fn version_rebase_by_branch(
            &self,
            base_version: VersionName,
            branch_name: BranchName,
        ) -> Result<()> {
            self.inner
                .version_rebase_by_branch(base_version, branch_name)
                .c(d!())
        }

        /// Create a new branch based on the head of the default branch.
        #[inline(always)]
        fn branch_create(
            &self,
            branch_name: BranchName,
            version_name: VersionName,
        ) -> Result<()> {
            self.inner.branch_create(branch_name, version_name).c(d!())
        }

        /// Create a new branch based on the head of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch(
            &self,
            branch_name: BranchName,
            version_name: VersionName,
            base_branch_name: ParentBranchName,
        ) -> Result<()> {
            self.inner
                .branch_create_by_base_branch(
                    branch_name,
                    version_name,
                    base_branch_name,
                )
                .c(d!())
        }

        /// Create a new branch based on a specified version of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &self,
            branch_name: BranchName,
            version_name: VersionName,
            base_branch_name: ParentBranchName,
            base_version_name: VersionName,
        ) -> Result<()> {
            self.inner
                .branch_create_by_base_branch_version(
                    branch_name,
                    version_name,
                    base_branch_name,
                    base_version_name,
                )
                .c(d!())
        }

        /// Check if a branch exists or not.
        #[inline(always)]
        fn branch_exists(&self, branch_name: BranchName) -> bool {
            self.inner.branch_exists(branch_name)
        }

        /// Check if a branch exists and has versions on it.
        fn branch_has_versions(&self, branch_name: BranchName) -> bool {
            self.inner.branch_has_versions(branch_name)
        }

        /// Remove a branch, remove all changes directly made by this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_remove(&self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_remove(branch_name).c(d!())
        }

        /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_truncate(&self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_truncate(branch_name).c(d!())
        }

        /// Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_truncate_to(
            &self,
            branch_name: BranchName,
            last_version_name: VersionName,
        ) -> Result<()> {
            self.inner
                .branch_truncate_to(branch_name, last_version_name)
                .c(d!())
        }

        /// Remove the newest version on a specified branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_pop_version(&self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_pop_version(branch_name).c(d!())
        }

        /// Merge a branch into another
        #[inline(always)]
        fn branch_merge_to(
            &self,
            branch_name: BranchName,
            target_branch_name: BranchName,
        ) -> Result<()> {
            self.inner
                .branch_merge_to(branch_name, target_branch_name)
                .c(d!())
        }

        /// Merge a branch into another,
        /// even if new different versions have been created on the target branch.
        ///
        /// # Safety
        ///
        /// If new different versions have been created on the target branch,
        /// the data records referenced by other branches may be corrupted.
        unsafe fn branch_merge_to_force(
            &self,
            branch_name: BranchName,
            target_branch_name: BranchName,
        ) -> Result<()> {
            self.inner
                .branch_merge_to_force(branch_name, target_branch_name)
                .c(d!())
        }

        /// Make a branch to be default,
        /// all default operations will be applied to it.
        #[inline(always)]
        fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_set_default(branch_name).c(d!())
        }

        /// Clean outdated versions out of the default reserved number.
        #[inline(always)]
        fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
            self.inner.prune(reserved_ver_num).c(d!())
        }
    };
}

/// Add nope implementations of `VsMgmt`
/// for types that are not defined in VSDB.
#[macro_export]
macro_rules! impl_vs_methods_nope {
    () => {
        #[inline(always)]
        fn version_create(&self, _: VersionName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_create_by_branch(
            &self,
            _: VersionName,
            __: BranchName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_exists(&self, _: VersionName) -> bool {
            true
        }

        #[inline(always)]
        fn version_exists_on_branch(&self, _: VersionName, __: BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn version_pop(&self) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_pop_by_branch(&self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        unsafe fn version_rebase(&self, _: VersionName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        unsafe fn version_rebase_by_branch(
            &self,
            _: VersionName,
            _: BranchName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create(&self, _: BranchName, _: VersionName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch(
            &self,
            _: BranchName,
            _: VersionName,
            _: ParentBranchName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &self,
            _: BranchName,
            _: VersionName,
            _: ParentBranchName,
            _: VersionName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_exists(&self, _: BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn branch_has_versions(&self, _: BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn branch_remove(&self, _: BranchName) -> Result<()> {
            Ok(())
        }
        #[inline(always)]
        fn branch_truncate(&self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_truncate_to(&self, _: BranchName, _: VersionName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_pop_version(&self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_merge_to(&self, _: BranchName, _: BranchName) -> Result<()> {
            Ok(())
        }

        unsafe fn branch_merge_to_force(
            &self,
            _: BranchName,
            _: BranchName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_set_default(&mut self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn prune(&self, _: Option<usize>) -> Result<()> {
            Ok(())
        }
    };
}

impl<T: ?Sized> VsMgmt for PhantomData<T> {
    impl_vs_methods_nope!();
}

impl<K, V> VsMgmt for Mapx<K, V> {
    impl_vs_methods_nope!();
}

impl<K, V> VsMgmt for MapxOrd<K, V> {
    impl_vs_methods_nope!();
}

impl<V> VsMgmt for MapxOrdRawKey<V> {
    impl_vs_methods_nope!();
}

impl<K> VsMgmt for MapxOrdRawValue<K> {
    impl_vs_methods_nope!();
}

impl VsMgmt for MapxRaw {
    impl_vs_methods_nope!();
}

impl<T> VsMgmt for Orphan<T> {
    impl_vs_methods_nope!();
}

impl<V> VsMgmt for Vecx<V> {
    impl_vs_methods_nope!();
}

impl VsMgmt for VecxRaw {
    impl_vs_methods_nope!();
}

#[cfg(feature = "merkle")]
impl VsMgmt for MerkleTree {
    impl_vs_methods_nope!();
}

#[cfg(feature = "merkle")]
impl VsMgmt for MerkleTreeStore {
    impl_vs_methods_nope!();
}

#[cfg(feature = "merkle")]
impl<'a> VsMgmt for Proof<'a> {
    impl_vs_methods_nope!();
}

#[cfg(feature = "merkle")]
impl<'a> VsMgmt for ProofEntry<'a> {
    impl_vs_methods_nope!();
}

macro_rules! impl_for_primitives {
    ($ty: ty) => {
        impl VsMgmt for $ty {
            impl_vs_methods_nope!();
        }
        impl VsMgmt for dyn AsRef<$ty> {
            impl_vs_methods_nope!();
        }
        impl VsMgmt for Box<dyn AsRef<$ty>> {
            impl_vs_methods_nope!();
        }
        impl VsMgmt for dyn AsRef<[$ty]> {
            impl_vs_methods_nope!();
        }
        impl VsMgmt for Box<dyn AsRef<[$ty]>> {
            impl_vs_methods_nope!();
        }
        impl<K> VsMgmt for HashMap<K, $ty> {
            impl_vs_methods_nope!();
        }
        impl<K> VsMgmt for BTreeMap<K, $ty> {
            impl_vs_methods_nope!();
        }
    };
    ($ty: ty, $($t: ty),+) => {
        impl_for_primitives!($ty);
        impl_for_primitives!(Box<[$ty]>);
        impl_for_primitives!(Vec<$ty>);
        impl_for_primitives!(VecDeque<$ty>);
        impl_for_primitives!(HashSet<$ty>);
        impl_for_primitives!(BTreeSet<$ty>);
        impl_for_primitives!(BinaryHeap<$ty>);
        impl_for_primitives!(LinkedList<$ty>);
        impl_for_primitives!($($t), +);
    };
}

impl_for_primitives!(
    i8,
    u8,
    i16,
    u16,
    i32,
    u32,
    i64,
    u64,
    i128,
    u128,
    isize,
    usize,
    bool,
    (),
    AtomicBool,
    AtomicI16,
    AtomicI32,
    AtomicI64,
    AtomicI8,
    AtomicU16,
    AtomicU32,
    AtomicU64,
    AtomicU8,
    primitive_types_0_10::U128,
    primitive_types_0_10::U256,
    primitive_types_0_10::U512,
    primitive_types_0_10::H128,
    primitive_types_0_10::H160,
    primitive_types_0_10::H256,
    primitive_types_0_10::H512,
    primitive_types_0_11::U128,
    primitive_types_0_11::U256,
    primitive_types_0_11::U512,
    primitive_types_0_11::H128,
    primitive_types_0_11::H160,
    primitive_types_0_11::H256,
    primitive_types_0_11::H512
);

impl<T: VsMgmt> VsMgmt for Option<T> {
    fn version_create(&self, version_name: VersionName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_create(version_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_create_by_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_create_by_branch(version_name, branch_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_exists(&self, version_name: VersionName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.version_exists(version_name);
        }
        true
    }

    #[inline(always)]
    fn version_exists_on_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> bool {
        if let Some(i) = self.as_ref() {
            return i.version_exists_on_branch(version_name, branch_name);
        }
        true
    }

    #[inline(always)]
    fn version_pop(&self) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_pop().c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_pop_by_branch(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn version_rebase(&self, base_version: VersionName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_rebase(base_version).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn version_rebase_by_branch(
        &self,
        base_version: VersionName,
        branch_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.version_rebase_by_branch(base_version, branch_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create(branch_name, version_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create_by_base_branch(branch_name, version_name, base_branch_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create_by_base_branch_version(
                branch_name,
                version_name,
                base_branch_name,
                base_version_name,
            )
            .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_exists(&self, branch_name: BranchName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.branch_exists(branch_name);
        }
        true // always return true if nope
    }

    #[inline(always)]
    fn branch_has_versions(&self, branch_name: BranchName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.branch_has_versions(branch_name);
        }
        true // always return true if nope
    }

    #[inline(always)]
    fn branch_remove(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_remove(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_truncate(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_truncate(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_truncate_to(
        &self,
        branch_name: BranchName,
        last_version_name: VersionName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_truncate_to(branch_name, last_version_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_pop_version(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_pop_version(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_merge_to(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_merge_to(branch_name, target_branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn branch_merge_to_force(
        &self,
        branch_name: BranchName,
        target_branch_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_merge_to_force(branch_name, target_branch_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_set_default(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.prune(reserved_ver_num).c(d!())?;
        }
        Ok(())
    }
}
