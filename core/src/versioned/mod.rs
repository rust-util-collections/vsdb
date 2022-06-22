//!
//! Versioned functions.
//!

pub mod mapx_raw;

use crate::{
    basic::mapx_raw::MapxRaw, BranchName, BranchNameOwned, ParentBranchName,
    VersionName, VersionNameOwned,
};
use ruc::*;
use std::{
    collections::{
        BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque,
    },
    marker::{PhantomData, PhantomPinned},
    sync::atomic::{
        AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicU16, AtomicU32,
        AtomicU64, AtomicU8,
    },
};

/// Methods collection of version management.
pub trait VsMgmt {
    /// Create a new version on the default branch.
    fn version_create(&mut self, ver_name: VersionName) -> Result<()>;

    /// Create a new version on a specified branch,
    /// NOTE: the branch must has been created.
    fn version_create_by_branch(
        &mut self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> Result<()>;

    /// Check if a verison exists on default branch.
    fn version_exists(&self, ver_name: VersionName) -> bool;

    /// Check if a version exists on a specified branch(include its parents).
    fn version_exists_on_branch(
        &self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> bool;

    /// Remove the newest version on the default branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn version_pop(&mut self) -> Result<()>;

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn version_pop_by_branch(&mut self, br_name: BranchName) -> Result<()>;

    /// Merge all changes made by new versions after the base version into the base version.
    ///
    /// # Safety
    ///
    /// It's the caller's duty to ensure that
    /// the `base_version` was created directly by the `br_id`,
    /// or the data records of other branches may be corrupted.
    unsafe fn version_rebase(&mut self, base_version: VersionName) -> Result<()>;

    /// Merge all changes made by new versions after the base version into the base version.
    ///
    /// # Safety
    ///
    /// It's the caller's duty to ensure that
    /// the `base_version` was created directly by the `br_id`,
    /// or the data records of other branches may be corrupted.
    unsafe fn version_rebase_by_branch(
        &mut self,
        base_version: VersionName,
        br_name: BranchName,
    ) -> Result<()>;

    /// Check if a version exists.
    fn version_exists_globally(&self, ver_name: VersionName) -> bool;

    /// # NOTE
    ///
    /// The result can only be used as hints, they are unreliable!
    ///
    /// For example, there are three Vs-structures:
    ///
    /// ```ignore
    /// struct Vs0(Vs1, Vs2);
    /// struct Vs1;
    /// struct Vs2
    /// ```
    /// the caller of `Vs0` can NOT guarantee that
    /// other callers have never created new branches and versions on `Vs1` or `Vs2`,
    /// so the results returned by `Vs1` and `Vs2` may be different,
    /// so `Vs0` can NOT guarantee that it can get a completely consistent result.
    fn version_list(&self) -> Result<Vec<VersionNameOwned>>;

    /// # NOTE
    ///
    /// The result can only be used as hints, they are unreliable!
    ///
    /// For example, there are three Vs-structures:
    ///
    /// ```ignore
    /// struct Vs0(Vs1, Vs2);
    /// struct Vs1;
    /// struct Vs2
    /// ```
    /// the caller of `Vs0` can NOT guarantee that
    /// other callers have never created new branches and versions on `Vs1` or `Vs2`,
    /// so the results returned by `Vs1` and `Vs2` may be different,
    /// so `Vs0` can NOT guarantee that it can get a completely consistent result.
    fn version_list_by_branch(
        &self,
        br_name: BranchName,
    ) -> Result<Vec<VersionNameOwned>>;

    /// # NOTE
    ///
    /// The result can only be used as hints, they are unreliable!
    ///
    /// For example, there are three Vs-structures:
    ///
    /// ```ignore
    /// struct Vs0(Vs1, Vs2);
    /// struct Vs1;
    /// struct Vs2
    /// ```
    /// the caller of `Vs0` can NOT guarantee that
    /// other callers have never created new branches and versions on `Vs1` or `Vs2`,
    /// so the results returned by `Vs1` and `Vs2` may be different,
    /// so `Vs0` can NOT guarantee that it can get a completely consistent result.
    fn version_list_globally(&self) -> Vec<VersionNameOwned>;

    /// Check if some changes have been make on the version.
    fn version_has_change_set(&self, ver_name: VersionName) -> Result<bool>;

    /// Clean up all orphan versions, versions not belong to any branch.
    fn version_clean_up_globally(&mut self) -> Result<()>;

    /// # Safety
    ///
    /// Version itself and its corresponding changes will be completely purged from all branches
    unsafe fn version_revert_globally(&mut self, ver_name: VersionName) -> Result<()>;

    /// Generate a trie root of the changes **directly** made by the target version,
    /// if no target version specified, the header version of the target branch will be used,
    /// if not target branch specified, then use the default branch.
    ///
    /// The returned result may be a single `vec![u8; 32]` or a concat of many `vec![u8; 32]`.
    fn version_chgset_trie_root(
        &self,
        br_name: Option<BranchName>,
        ver_name: Option<VersionName>,
    ) -> Result<Vec<u8>>;

    /// Create a new branch based on the head of the default branch.
    fn branch_create(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        force: bool,
    ) -> Result<()>;

    /// Create a new branch based on the head of a specified branch.
    fn branch_create_by_base_branch(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()>;

    /// Create a new branch based on a specified version of a specified branch.
    fn branch_create_by_base_branch_version(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()>;

    /// # Safety
    ///
    /// You should create a new version manually before writing to the new branch,
    /// or the data records referenced by other branches may be corrupted.
    unsafe fn branch_create_without_new_version(
        &mut self,
        br_name: BranchName,
        force: bool,
    ) -> Result<()>;

    /// # Safety
    ///
    /// You should create a new version manually before writing to the new branch,
    /// or the data records referenced by other branches may be corrupted.
    unsafe fn branch_create_by_base_branch_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()>;

    /// # Safety
    ///
    /// You should create a new version manually before writing to the new branch,
    /// or the data records referenced by other branches may be corrupted.
    unsafe fn branch_create_by_base_branch_version_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()>;

    /// Check if a branch exists or not.
    fn branch_exists(&self, br_name: BranchName) -> bool;

    /// Check if a branch exists and has versions on it.
    fn branch_has_versions(&self, br_name: BranchName) -> bool;

    /// Remove a branch, remove all changes directly made by this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_remove(&mut self, br_name: BranchName) -> Result<()>;

    /// Clean up all other branches not in the list,
    /// will also clean up all orphan versions.
    fn branch_keep_only(&mut self, br_names: &[BranchName]) -> Result<()>;

    /// Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_truncate(&mut self, br_name: BranchName) -> Result<()>;

    /// Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_truncate_to(
        &mut self,
        br_name: BranchName,
        last_ver_name: VersionName,
    ) -> Result<()>;

    /// Remove the newest version on a specified branch.
    ///
    /// 'Write'-like operations on branches and versions are different from operations on data.
    ///
    /// 'Write'-like operations on data require recursive tracing of all parent nodes,
    /// while operations on branches and versions are limited to their own perspective,
    /// and should not do any tracing.
    fn branch_pop_version(&mut self, br_name: BranchName) -> Result<()>;

    /// Merge a branch into another.
    fn branch_merge_to(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()>;

    /// Merge a branch into another,
    /// even if new different versions have been created on the target branch.
    ///
    /// # Safety
    ///
    /// If new different versions have been created on the target branch,
    /// the data records referenced by other branches may be corrupted.
    unsafe fn branch_merge_to_force(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()>;

    /// Make a branch to be default,
    /// all default operations will be applied to it.
    fn branch_set_default(&mut self, br_name: BranchName) -> Result<()>;

    /// Check if the branch has no versions or only empty versions.
    fn branch_is_empty(&self, br_name: BranchName) -> Result<bool>;

    /// # NOTE
    ///
    /// The result can only be used as hints, they are unreliable!
    ///
    /// For example, there are three Vs-structures:
    ///
    /// ```ignore
    /// struct Vs0(Vs1, Vs2);
    /// struct Vs1;
    /// struct Vs2
    /// ```
    /// the caller of `Vs0` can NOT guarantee that
    /// other callers have never created new branches and versions on `Vs1` or `Vs2`,
    /// so the results returned by `Vs1` and `Vs2` may be different,
    /// so `Vs0` can NOT guarantee that it can get a completely consistent result.
    fn branch_list(&self) -> Vec<BranchNameOwned>;

    /// Get the default branch name.
    fn branch_get_default(&self) -> BranchNameOwned;

    /// Logically similar to `std::ptr::swap`
    ///
    /// For example: If you have a master branch and a test branch, the data is always trial-run on the test branch, and then periodically merged back into the master branch. Rather than merging the test branch into the master branch, and then recreating the new test branch, it is more efficient to just swap the two branches, and then recreating the new test branch.
    ///
    /// # Safety
    ///
    /// - Non-'thread safe'
    /// - Must ensure that there are no reads and writes to these two branches during the execution
    unsafe fn branch_swap(
        &mut self,
        branch_1: BranchName,
        branch_2: BranchName,
    ) -> Result<()>;

    /// Clean outdated versions out of the default branch.
    fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()>;
}

#[macro_export]
macro_rules! impl_vs_methods {
    () => {
        /// Create a new version on the default branch.
        #[inline(always)]
        fn version_create(&mut self, ver_name: $crate::VersionName) -> ruc::Result<()> {
            self.inner.version_create(ver_name).c(d!())
        }

        /// Create a new version on a specified branch,
        /// NOTE: the branch must has been created.
        #[inline(always)]
        fn version_create_by_branch(
            &mut self,
            ver_name: $crate::VersionName,
            br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner
                .version_create_by_branch(ver_name, br_name)
                .c(d!())
        }

        /// Check if a verison exists on default branch.
        #[inline(always)]
        fn version_exists(&self, ver_name: $crate::VersionName) -> bool {
            self.inner.version_exists(ver_name)
        }

        /// Check if a version exists on a specified branch(include its parents).
        #[inline(always)]
        fn version_exists_on_branch(
            &self,
            ver_name: $crate::VersionName,
            br_name: $crate::BranchName,
        ) -> bool {
            self.inner.version_exists_on_branch(ver_name, br_name)
        }

        /// Remove the newest version on the default branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn version_pop(&mut self) -> ruc::Result<()> {
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
        fn version_pop_by_branch(
            &mut self,
            br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner.version_pop_by_branch(br_name).c(d!())
        }

        /// Merge all changes made by new versions after the base version into the base version.
        ///
        /// # Safety
        ///
        /// It's the caller's duty to ensure that
        /// the `base_version` was created directly by the `br_id`,
        /// or the data records of other branches may be corrupted.
        #[inline(always)]
        unsafe fn version_rebase(
            &mut self,
            base_version: $crate::VersionName,
        ) -> ruc::Result<()> {
            self.inner.version_rebase(base_version).c(d!())
        }

        /// Merge all changes made by new versions after the base version into the base version.
        ///
        /// # Safety
        ///
        /// It's the caller's duty to ensure that
        /// the `base_version` was created directly by the `br_id`,
        /// or the data records of other branches may be corrupted.
        #[inline(always)]
        unsafe fn version_rebase_by_branch(
            &mut self,
            base_version: $crate::VersionName,
            br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner
                .version_rebase_by_branch(base_version, br_name)
                .c(d!())
        }

        #[inline(always)]
        fn version_exists_globally(&self, ver_name: $crate::VersionName) -> bool {
            self.inner.version_exists_globally(ver_name)
        }

        #[inline(always)]
        fn version_list(&self) -> ruc::Result<Vec<$crate::VersionNameOwned>> {
            self.inner.version_list().c(d!())
        }

        #[inline(always)]
        fn version_list_by_branch(
            &self,
            br_name: $crate::BranchName,
        ) -> ruc::Result<Vec<$crate::VersionNameOwned>> {
            self.inner.version_list_by_branch(br_name).c(d!())
        }

        #[inline(always)]
        fn version_list_globally(&self) -> Vec<$crate::VersionNameOwned> {
            self.inner.version_list_globally()
        }

        #[inline(always)]
        fn version_has_change_set(
            &self,
            ver_name: $crate::VersionName,
        ) -> ruc::Result<bool> {
            self.inner.version_has_change_set(ver_name).c(d!())
        }

        #[inline(always)]
        fn version_clean_up_globally(&mut self) -> ruc::Result<()> {
            self.inner.version_clean_up_globally().c(d!())
        }

        #[inline(always)]
        unsafe fn version_revert_globally(
            &mut self,
            ver_name: $crate::VersionName,
        ) -> ruc::Result<()> {
            self.inner.version_revert_globally(ver_name).c(d!())
        }

        #[inline(always)]
        fn version_chgset_trie_root(
            &self,
            br_name: Option<BranchName>,
            ver_name: Option<VersionName>,
        ) -> Result<Vec<u8>> {
            self.inner
                .version_chgset_trie_root(br_name, ver_name)
                .c(d!())
        }

        /// Create a new branch based on the head of the default branch.
        #[inline(always)]
        fn branch_create(
            &mut self,
            br_name: $crate::BranchName,
            ver_name: $crate::VersionName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner.branch_create(br_name, ver_name, force).c(d!())
        }

        /// Create a new branch based on the head of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch(
            &mut self,
            br_name: $crate::BranchName,
            ver_name: $crate::VersionName,
            base_br_name: $crate::ParentBranchName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner
                .branch_create_by_base_branch(br_name, ver_name, base_br_name, force)
                .c(d!())
        }

        /// Create a new branch based on a specified version of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &mut self,
            br_name: $crate::BranchName,
            ver_name: $crate::VersionName,
            base_br_name: $crate::ParentBranchName,
            base_ver_name: $crate::VersionName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner
                .branch_create_by_base_branch_version(
                    br_name,
                    ver_name,
                    base_br_name,
                    base_ver_name,
                    force,
                )
                .c(d!())
        }

        /// # Safety
        ///
        /// You should create a new version manually before writing to the new branch,
        /// or the data records referenced by other branches may be corrupted.
        #[inline(always)]
        unsafe fn branch_create_without_new_version(
            &mut self,
            br_name: $crate::BranchName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner
                .branch_create_without_new_version(br_name, force)
                .c(d!())
        }

        /// # Safety
        ///
        /// You should create a new version manually before writing to the new branch,
        /// or the data records referenced by other branches may be corrupted.
        #[inline(always)]
        unsafe fn branch_create_by_base_branch_without_new_version(
            &mut self,
            br_name: $crate::BranchName,
            base_br_name: $crate::ParentBranchName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner
                .branch_create_by_base_branch_without_new_version(
                    br_name,
                    base_br_name,
                    force,
                )
                .c(d!())
        }

        /// # Safety
        ///
        /// You should create a new version manually before writing to the new branch,
        /// or the data records referenced by other branches may be corrupted.
        #[inline(always)]
        unsafe fn branch_create_by_base_branch_version_without_new_version(
            &mut self,
            br_name: $crate::BranchName,
            base_br_name: $crate::ParentBranchName,
            base_ver_name: $crate::VersionName,
            force: bool,
        ) -> ruc::Result<()> {
            self.inner
                .branch_create_by_base_branch_version_without_new_version(
                    br_name,
                    base_br_name,
                    base_ver_name,
                    force,
                )
                .c(d!())
        }

        /// Check if a branch exists or not.
        #[inline(always)]
        fn branch_exists(&self, br_name: $crate::BranchName) -> bool {
            self.inner.branch_exists(br_name)
        }

        /// Check if a branch exists and has versions on it.
        fn branch_has_versions(&self, br_name: $crate::BranchName) -> bool {
            self.inner.branch_has_versions(br_name)
        }

        /// Remove a branch, remove all changes directly made by this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_remove(&mut self, br_name: $crate::BranchName) -> ruc::Result<()> {
            self.inner.branch_remove(br_name).c(d!())
        }

        /// Clean up all other branches not in the list.
        #[inline(always)]
        fn branch_keep_only(
            &mut self,
            br_names: &[$crate::BranchName],
        ) -> ruc::Result<()> {
            self.inner.branch_keep_only(br_names).c(d!())
        }

        /// Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_truncate(&mut self, br_name: $crate::BranchName) -> ruc::Result<()> {
            self.inner.branch_truncate(br_name).c(d!())
        }

        /// Remove all changes directly made by versions(bigger than `last_ver_id`) of this branch.
        ///
        /// 'Write'-like operations on branches and versions are different from operations on data.
        ///
        /// 'Write'-like operations on data require recursive tracing of all parent nodes,
        /// while operations on branches and versions are limited to their own perspective,
        /// and should not do any tracing.
        #[inline(always)]
        fn branch_truncate_to(
            &mut self,
            br_name: $crate::BranchName,
            last_ver_name: $crate::VersionName,
        ) -> ruc::Result<()> {
            self.inner
                .branch_truncate_to(br_name, last_ver_name)
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
        fn branch_pop_version(
            &mut self,
            br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner.branch_pop_version(br_name).c(d!())
        }

        /// Merge a branch into another
        #[inline(always)]
        fn branch_merge_to(
            &mut self,
            br_name: $crate::BranchName,
            target_br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner.branch_merge_to(br_name, target_br_name).c(d!())
        }

        /// Merge a branch into another,
        /// even if new different versions have been created on the target branch.
        ///
        /// # Safety
        ///
        /// If new different versions have been created on the target branch,
        /// the data records referenced by other branches may be corrupted.
        unsafe fn branch_merge_to_force(
            &mut self,
            br_name: $crate::BranchName,
            target_br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner
                .branch_merge_to_force(br_name, target_br_name)
                .c(d!())
        }

        /// Make a branch to be default,
        /// all default operations will be applied to it.
        #[inline(always)]
        fn branch_set_default(
            &mut self,
            br_name: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner.branch_set_default(br_name).c(d!())
        }

        fn branch_is_empty(&self, br_name: $crate::BranchName) -> ruc::Result<bool> {
            self.inner.branch_is_empty(br_name).c(d!())
        }

        fn branch_list(&self) -> Vec<$crate::BranchNameOwned> {
            self.inner.branch_list()
        }

        fn branch_get_default(&self) -> $crate::BranchNameOwned {
            self.inner.branch_get_default()
        }

        unsafe fn branch_swap(
            &mut self,
            branch_1: $crate::BranchName,
            branch_2: $crate::BranchName,
        ) -> ruc::Result<()> {
            self.inner.branch_swap(branch_1, branch_2).c(d!())
        }

        /// Clean outdated versions out of the default reserved number.
        #[inline(always)]
        fn prune(&mut self, reserved_ver_num: Option<usize>) -> ruc::Result<()> {
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
        fn version_create(&mut self, _: $crate::VersionName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_create_by_branch(
            &mut self,
            _: $crate::VersionName,
            __: $crate::BranchName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_exists(&self, _: $crate::VersionName) -> bool {
            true
        }

        #[inline(always)]
        fn version_exists_on_branch(
            &self,
            _: $crate::VersionName,
            __: $crate::BranchName,
        ) -> bool {
            true
        }

        #[inline(always)]
        fn version_pop(&mut self) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_pop_by_branch(&mut self, _: $crate::BranchName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        unsafe fn version_rebase(&mut self, _: $crate::VersionName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        unsafe fn version_rebase_by_branch(
            &mut self,
            _: $crate::VersionName,
            _: $crate::BranchName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        fn version_exists_globally(&self, _: $crate::VersionName) -> bool {
            true
        }

        fn version_list(&self) -> ruc::Result<Vec<$crate::VersionNameOwned>> {
            Ok(Default::default())
        }

        fn version_list_by_branch(
            &self,
            _: $crate::BranchName,
        ) -> ruc::Result<Vec<$crate::VersionNameOwned>> {
            Ok(Default::default())
        }

        fn version_list_globally(&self) -> Vec<$crate::VersionNameOwned> {
            Default::default()
        }

        fn version_has_change_set(&self, _: $crate::VersionName) -> ruc::Result<bool> {
            Ok(true)
        }

        fn version_clean_up_globally(&mut self) -> ruc::Result<()> {
            Ok(())
        }

        unsafe fn version_revert_globally(
            &mut self,
            _: $crate::VersionName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn version_chgset_trie_root(
            &self,
            _: Option<$crate::BranchName>,
            _: Option<$crate::VersionName>,
        ) -> ruc::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        #[inline(always)]
        fn branch_create(
            &mut self,
            _: $crate::BranchName,
            _: $crate::VersionName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch(
            &mut self,
            _: $crate::BranchName,
            _: $crate::VersionName,
            _: $crate::ParentBranchName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &mut self,
            _: $crate::BranchName,
            _: $crate::VersionName,
            _: $crate::ParentBranchName,
            _: $crate::VersionName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        unsafe fn branch_create_without_new_version(
            &mut self,
            _: $crate::BranchName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        unsafe fn branch_create_by_base_branch_without_new_version(
            &mut self,
            _: $crate::BranchName,
            _: $crate::ParentBranchName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        unsafe fn branch_create_by_base_branch_version_without_new_version(
            &mut self,
            _: $crate::BranchName,
            _: $crate::ParentBranchName,
            _: $crate::VersionName,
            _: bool,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_exists(&self, _: $crate::BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn branch_has_versions(&self, _: $crate::BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn branch_remove(&mut self, _: $crate::BranchName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_keep_only(&mut self, _: &[$crate::BranchName]) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_truncate(&mut self, _: $crate::BranchName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_truncate_to(
            &mut self,
            _: $crate::BranchName,
            _: $crate::VersionName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_pop_version(&mut self, _: $crate::BranchName) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_merge_to(
            &mut self,
            _: $crate::BranchName,
            _: $crate::BranchName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        unsafe fn branch_merge_to_force(
            &mut self,
            _: $crate::BranchName,
            _: $crate::BranchName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_set_default(&mut self, _: $crate::BranchName) -> ruc::Result<()> {
            Ok(())
        }

        fn branch_is_empty(&self, _: $crate::BranchName) -> ruc::Result<bool> {
            Ok(true)
        }

        fn branch_list(&self) -> Vec<$crate::BranchNameOwned> {
            Default::default()
        }

        fn branch_get_default(&self) -> $crate::BranchNameOwned {
            Default::default()
        }

        unsafe fn branch_swap(
            &mut self,
            _: $crate::BranchName,
            _: $crate::BranchName,
        ) -> ruc::Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn prune(&mut self, _: Option<usize>) -> ruc::Result<()> {
            Ok(())
        }
    };
}

impl VsMgmt for MapxRaw {
    impl_vs_methods_nope!();
}

impl<T> VsMgmt for PhantomData<T> {
    impl_vs_methods_nope!();
}

impl VsMgmt for PhantomPinned {
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
    AtomicU8
);

#[cfg(feature = "extra_types")]
impl_for_primitives!(
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
    fn version_create(&mut self, ver_name: VersionName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_create(ver_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_create_by_branch(
        &mut self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_create_by_branch(ver_name, br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_exists(&self, ver_name: VersionName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.version_exists(ver_name);
        }
        true
    }

    #[inline(always)]
    fn version_exists_on_branch(
        &self,
        ver_name: VersionName,
        br_name: BranchName,
    ) -> bool {
        if let Some(i) = self.as_ref() {
            return i.version_exists_on_branch(ver_name, br_name);
        }
        true
    }

    #[inline(always)]
    fn version_pop(&mut self) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_pop().c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_pop_by_branch(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_pop_by_branch(br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn version_rebase(&mut self, base_version: VersionName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_rebase(base_version).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn version_rebase_by_branch(
        &mut self,
        base_version: VersionName,
        br_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_rebase_by_branch(base_version, br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_exists_globally(&self, ver_name: VersionName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.version_exists_globally(ver_name);
        }
        true
    }

    #[inline(always)]
    fn version_list(&self) -> Result<Vec<VersionNameOwned>> {
        if let Some(i) = self.as_ref() {
            i.version_list().c(d!())?;
        }
        Ok(Default::default())
    }

    #[inline(always)]
    fn version_list_by_branch(
        &self,
        br_name: BranchName,
    ) -> Result<Vec<VersionNameOwned>> {
        if let Some(i) = self.as_ref() {
            i.version_list_by_branch(br_name).c(d!())?;
        }
        Ok(Default::default())
    }

    #[inline(always)]
    fn version_list_globally(&self) -> Vec<VersionNameOwned> {
        if let Some(i) = self.as_ref() {
            return i.version_list_globally();
        }
        Default::default()
    }

    #[inline(always)]
    fn version_has_change_set(&self, ver_name: VersionName) -> Result<bool> {
        if let Some(i) = self.as_ref() {
            i.version_has_change_set(ver_name).c(d!())?;
        }
        Ok(true)
    }

    #[inline(always)]
    fn version_clean_up_globally(&mut self) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_clean_up_globally().c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn version_revert_globally(&mut self, ver_name: VersionName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.version_revert_globally(ver_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn version_chgset_trie_root(
        &self,
        br_name: Option<BranchName>,
        ver_name: Option<VersionName>,
    ) -> Result<Vec<u8>> {
        if let Some(i) = self.as_ref() {
            i.version_chgset_trie_root(br_name, ver_name).c(d!())?;
        }
        Ok(Vec::new())
    }

    #[inline(always)]
    fn branch_create(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create(br_name, ver_name, force).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create_by_base_branch(br_name, ver_name, base_br_name, force)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &mut self,
        br_name: BranchName,
        ver_name: VersionName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create_by_base_branch_version(
                br_name,
                ver_name,
                base_br_name,
                base_ver_name,
                force,
            )
            .c(d!())?;
        }
        Ok(())
    }

    unsafe fn branch_create_without_new_version(
        &mut self,
        br_name: BranchName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create_without_new_version(br_name, force)
                .c(d!())?;
        }
        Ok(())
    }

    unsafe fn branch_create_by_base_branch_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create_by_base_branch_without_new_version(
                br_name,
                base_br_name,
                force,
            )
            .c(d!())?;
        }
        Ok(())
    }

    unsafe fn branch_create_by_base_branch_version_without_new_version(
        &mut self,
        br_name: BranchName,
        base_br_name: ParentBranchName,
        base_ver_name: VersionName,
        force: bool,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_create_by_base_branch_version_without_new_version(
                br_name,
                base_br_name,
                base_ver_name,
                force,
            )
            .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_exists(&self, br_name: BranchName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.branch_exists(br_name);
        }
        true // always return true if nope
    }

    #[inline(always)]
    fn branch_has_versions(&self, br_name: BranchName) -> bool {
        if let Some(i) = self.as_ref() {
            return i.branch_has_versions(br_name);
        }
        true // always return true if nope
    }

    #[inline(always)]
    fn branch_remove(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_remove(br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_keep_only(&mut self, br_names: &[BranchName]) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_keep_only(br_names).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_truncate(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_truncate(br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_truncate_to(
        &mut self,
        br_name: BranchName,
        last_ver_name: VersionName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_truncate_to(br_name, last_ver_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_pop_version(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_pop_version(br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_merge_to(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_merge_to(br_name, target_br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    unsafe fn branch_merge_to_force(
        &mut self,
        br_name: BranchName,
        target_br_name: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_merge_to_force(br_name, target_br_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_set_default(&mut self, br_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_set_default(br_name).c(d!())?;
        }
        Ok(())
    }

    fn branch_is_empty(&self, br_name: BranchName) -> Result<bool> {
        if let Some(i) = self.as_ref() {
            i.branch_is_empty(br_name).c(d!())?;
        }
        Ok(true)
    }

    fn branch_list(&self) -> Vec<BranchNameOwned> {
        if let Some(i) = self.as_ref() {
            return i.branch_list();
        }
        Default::default()
    }

    fn branch_get_default(&self) -> BranchNameOwned {
        if let Some(i) = self.as_ref() {
            return i.branch_get_default();
        }
        Default::default()
    }

    unsafe fn branch_swap(
        &mut self,
        branch_1: BranchName,
        branch_2: BranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.branch_swap(branch_1, branch_2).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
        if let Some(i) = self.as_mut() {
            i.prune(reserved_ver_num).c(d!())?;
        }
        Ok(())
    }
}

// impl<T: VsMgmt + Deref + DerefMut> VsMgmt for Pin<T> {
//     fn version_create(&mut self, ver_name: VersionName) -> Result<()> {
//         self.as_mut().version_create(ver_name).c(d!())
//     }
//
//     #[inline(always)]
//     fn version_create_by_branch(
//         &mut self,
//         ver_name: VersionName,
//         br_name: BranchName,
//     ) -> Result<()> {
//         self.as_mut()
//             .version_create_by_branch(ver_name, br_name)
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn version_exists(&self, ver_name: VersionName) -> bool {
//         self.as_ref().version_exists(ver_name)
//     }
//
//     #[inline(always)]
//     fn version_exists_on_branch(
//         &self,
//         ver_name: VersionName,
//         br_name: BranchName,
//     ) -> bool {
//         self.as_ref()
//             .version_exists_on_branch(ver_name, br_name)
//     }
//
//     #[inline(always)]
//     fn version_pop(&mut self) -> Result<()> {
//         self.as_mut().version_pop().c(d!())
//     }
//
//     #[inline(always)]
//     fn version_pop_by_branch(&mut self, br_name: BranchName) -> Result<()> {
//         self.as_mut().version_pop_by_branch(br_name).c(d!())
//     }
//
//     #[inline(always)]
//     unsafe fn version_rebase(&mut self, base_version: VersionName) -> Result<()> {
//         self.as_mut().version_rebase(base_version).c(d!())
//     }
//
//     #[inline(always)]
//     unsafe fn version_rebase_by_branch(
//         &mut self,
//         base_version: VersionName,
//         br_name: BranchName,
//     ) -> Result<()> {
//         self.as_mut()
//             .version_rebase_by_branch(base_version, br_name)
//             .c(d!())
//     }
//
//     fn version_exists_globally(&self, ver_name: VersionName) -> bool {
//         self.as_ref().version_exists_globally(ver_name)
//     }
//
//     fn version_list(&self) -> Result<Vec<VersionNameOwned>> {
//         self.as_ref().version_list().c(d!())
//     }
//
//     fn version_list_by_branch(
//         &self,
//         br_name: BranchName,
//     ) -> Result<Vec<VersionNameOwned>> {
//         self.as_ref().version_list_by_branch(br_name).c(d!())
//     }
//
//     fn version_list_globally(&self) -> Vec<VersionNameOwned> {
//         self.as_ref().version_list_globally()
//     }
//
//     fn version_has_change_set(&self, ver_name: VersionName) -> Result<bool> {
//         self.as_ref().version_has_change_set(ver_name).c(d!())
//     }
//
//     fn version_clean_up_globally(&mut self) -> Result<()> {
//         self.as_mut().version_clean_up_globally().c(d!())
//     }
//
//     unsafe fn version_revert_globally(
//         &mut self,
//         ver_name: VersionName,
//     ) -> Result<()> {
//         self.as_mut().version_revert_globally(ver_name).c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_create(
//         &mut self,
//         br_name: BranchName,
//         ver_name: VersionName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create(br_name, ver_name, force)
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_create_by_base_branch(
//         &mut self,
//         br_name: BranchName,
//         ver_name: VersionName,
//         base_br_name: ParentBranchName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create_by_base_branch(
//                 br_name,
//                 ver_name,
//                 base_br_name,
//                 force,
//             )
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_create_by_base_branch_version(
//         &mut self,
//         br_name: BranchName,
//         ver_name: VersionName,
//         base_br_name: ParentBranchName,
//         base_ver_name: VersionName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create_by_base_branch_version(
//                 br_name,
//                 ver_name,
//                 base_br_name,
//                 base_ver_name,
//                 force,
//             )
//             .c(d!())
//     }
//
//     unsafe fn branch_create_without_new_version(
//         &mut self,
//         br_name: BranchName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create_without_new_version(br_name, force)
//             .c(d!())
//     }
//
//     unsafe fn branch_create_by_base_branch_without_new_version(
//         &mut self,
//         br_name: BranchName,
//         base_br_name: ParentBranchName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create_by_base_branch_without_new_version(
//                 br_name,
//                 base_br_name,
//                 force,
//             )
//             .c(d!())
//     }
//
//     unsafe fn branch_create_by_base_branch_version_without_new_version(
//         &mut self,
//         br_name: BranchName,
//         base_br_name: ParentBranchName,
//         base_ver_name: VersionName,
//         force: bool,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_create_by_base_branch_version_without_new_version(
//                 br_name,
//                 base_br_name,
//                 base_ver_name,
//                 force,
//             )
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_exists(&self, br_name: BranchName) -> bool {
//         self.as_ref().branch_exists(br_name)
//     }
//
//     #[inline(always)]
//     fn branch_has_versions(&self, br_name: BranchName) -> bool {
//         self.as_ref().branch_has_versions(br_name)
//     }
//
//     #[inline(always)]
//     fn branch_remove(&mut self, br_name: BranchName) -> Result<()> {
//         self.as_mut().branch_remove(br_name).c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_keep_only(&mut self, br_names: &[BranchName]) -> Result<()> {
//         self.as_mut().branch_keep_only(br_names).c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_truncate(&mut self, br_name: BranchName) -> Result<()> {
//         self.as_mut().branch_truncate(br_name).c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_truncate_to(
//         &mut self,
//         br_name: BranchName,
//         last_ver_name: VersionName,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_truncate_to(br_name, last_ver_name)
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_pop_version(&mut self, br_name: BranchName) -> Result<()> {
//         self.as_mut().branch_pop_version(br_name).c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_merge_to(
//         &mut self,
//         br_name: BranchName,
//         target_br_name: BranchName,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_merge_to(br_name, target_br_name)
//             .c(d!())
//     }
//
//     #[inline(always)]
//     unsafe fn branch_merge_to_force(
//         &mut self,
//         br_name: BranchName,
//         target_br_name: BranchName,
//     ) -> Result<()> {
//         self.as_mut()
//             .branch_merge_to_force(br_name, target_br_name)
//             .c(d!())
//     }
//
//     #[inline(always)]
//     fn branch_set_default(&mut self, br_name: BranchName) -> Result<()> {
//         self.as_mut().branch_set_default(br_name).c(d!())
//     }
//
//     fn branch_is_empty(&self, br_name: BranchName) -> Result<bool> {
//         self.as_ref().branch_is_empty(br_name).c(d!())
//     }
//
//     fn branch_list(&self) -> Vec<BranchNameOwned> {
//         self.as_ref().branch_list()
//     }
//
//     fn branch_get_default(&self) -> BranchNameOwned {
//         self.as_ref().branch_get_default()
//     }
//
//     unsafe fn branch_swap(
//         &mut self,
//         branch_1: BranchName,
//         branch_2: BranchName,
//     ) -> Result<()> {
//         self.as_mut().branch_swap(branch_1, branch_2).c(d!())
//     }
//
//     #[inline(always)]
//     fn prune(&mut self, reserved_ver_num: Option<usize>) -> Result<()> {
//         self.as_mut().prune(reserved_ver_num).c(d!())
//     }
// }
