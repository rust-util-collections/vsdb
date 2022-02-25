//!
//! Versioned functions.
//!
//! # Examples
//!
//! ```rust
//! use once_cell::sync::Lazy;
//! use ruc::*;
//! use serde::{Deserialize, Serialize};
//! use std::{
//!     mem,
//!     sync::{mpsc::channel, Mutex},
//!     thread,
//! };
//! use vsdb::{BranchName, MapxVs, OrphanVs, VecxVs, VersionName, Vs, VsMgmt, ValueEnDe};
//!
//! type Amount = u64;
//! type Address = Vec<u8>;
//! type ConsensusInt = i128;
//!
//! #[derive(Vs, Clone, Debug, Serialize, Deserialize)]
//! struct WorldState {
//!     transactions: VecxVs<Transaction>,
//!     balances: MapxVs<Address, Amount>,
//!     a_consensus_int: OrphanVs<ConsensusInt>,
//! }
//!
//! const MASTER_BRANCH: &str = "master";
//! const PRE_CHECK_BRANCH: &str = "pre_check";
//! const FORMAL_CHECK_BRANCH: &str = "formal_check";
//!
//! static WORLD_STATE: Lazy<Mutex<WorldState>> =
//!     Lazy::new(|| Mutex::new(WorldState::load_or_init().unwrap()));
//!
//! static WORLD_STATE_SNAP_0: Lazy<Mutex<WorldState>> = Lazy::new(|| {
//!     let mut snap0 = WORLD_STATE.lock().unwrap().clone();
//!     pnk!(snap0.reset_branch(PRE_CHECK_BRANCH));
//!     Mutex::new(snap0)
//! });
//!
//! static WORLD_STATE_SNAP_1: Lazy<Mutex<WorldState>> = Lazy::new(|| {
//!     let mut snap1 = WORLD_STATE.lock().unwrap().clone();
//!     pnk!(snap1.reset_branch(FORMAL_CHECK_BRANCH));
//!     Mutex::new(snap1)
//! });
//!
//! static MEM_POOL: Lazy<Mutex<Vec<Transaction>>> = Lazy::new(|| Mutex::new(vec![]));
//!
//! fn transaction_pre_check(tx: Transaction) {
//!     let mut snap0 = WORLD_STATE_SNAP_0.lock().unwrap();
//!     snap0.push_version(&tx.hash()).unwrap();
//!     if snap0.apply_transaction(&tx).is_ok() {
//!         MEM_POOL.lock().unwrap().push(tx);
//!     } else {
//!         snap0.version_pop().unwrap();
//!     }
//! }
//!
//! fn begin_block() {
//!     let mut snap0 = WORLD_STATE_SNAP_0.lock().unwrap();
//!     pnk!(snap0.reset_branch(PRE_CHECK_BRANCH));
//!     let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap();
//!     pnk!(snap1.reset_branch(FORMAL_CHECK_BRANCH));
//! }
//!
//! fn transaction_formal_check_all() {
//!     let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap().clone();
//!     for tx in mem::take(&mut *MEM_POOL.lock().unwrap()).into_iter() {
//!         snap1.push_version(&tx.hash()).unwrap();
//!         if snap1.apply_transaction(&tx).is_err() {
//!             snap1.version_pop().unwrap();
//!         }
//!     }
//! }
//!
//! fn end_block() {
//!     let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap();
//!     snap1.merge_branch(FORMAL_CHECK_BRANCH).unwrap();
//! }
//!
//! impl WorldState {
//!     // sample code
//!     fn load_or_init() -> Result<Self> {
//!         let mut ws = WorldState {
//!             transactions: VecxVs::new(),
//!             balances: MapxVs::new(),
//!             a_consensus_int: OrphanVs::new(0),
//!         };
//!
//!         if !ws.branch_is_found(MASTER_BRANCH) {
//!             ws.push_version(b"init version").c(d!())?;
//!             ws.new_branch(MASTER_BRANCH).c(d!())?;
//!         }
//!         ws.set_default_branch(MASTER_BRANCH).c(d!())?;
//!         ws.push_version(b"init version 2").c(d!())?;
//!
//!         ws.new_branch(PRE_CHECK_BRANCH).c(d!())?;
//!         ws.new_branch(FORMAL_CHECK_BRANCH).c(d!())?;
//!
//!         Ok(ws)
//!     }
//!
//!     fn apply_transaction(&mut self, tx: &Transaction) -> Result<()> {
//!         self.a_very_complex_function_will_change_state(tx).c(d!())
//!     }
//!
//!     // sample code
//!     fn a_very_complex_function_will_change_state(
//!         &mut self,
//!         tx: &Transaction,
//!     ) -> Result<()> {
//!         if tx.from.get(0).is_some() {
//!             Ok(())
//!         } else {
//!             // ..........
//!             Err(eg!("error occur"))
//!         }
//!     }
//!
//!     fn branch_is_found(&self, branch: &str) -> bool {
//!         let br = BranchName(branch.as_bytes());
//!         self.branch_exists(br)
//!     }
//!
//!     fn new_branch(&mut self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.branch_create(br).c(d!())
//!     }
//!
//!     fn delete_branch(&mut self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.branch_remove(br).c(d!())
//!     }
//!
//!     fn merge_branch(&mut self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.branch_merge_to_parent(br).c(d!())
//!     }
//!
//!     fn set_default_branch(&mut self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.branch_set_default(br).c(d!())
//!     }
//!
//!     fn reset_branch(&mut self, branch: &str) -> Result<()> {
//!         self.set_default_branch(MASTER_BRANCH)
//!             .c(d!())
//!             .and_then(|_| self.delete_branch(branch).c(d!()))
//!             .and_then(|_| self.new_branch(branch).c(d!()))
//!             .and_then(|_| self.set_default_branch(branch).c(d!()))
//!     }
//!
//!     fn push_version(&mut self, version: &[u8]) -> Result<()> {
//!         let ver = VersionName(version);
//!         self.version_create(ver).c(d!())
//!     }
//! }
//!
//! #[derive(Default, Clone, Debug, Serialize, Deserialize)]
//! struct Transaction {
//!     from: Address,
//!     to: Address,
//!     amount: Amount,
//! }
//!
//! impl Transaction {
//!     fn hash(&self) -> Vec<u8> {
//!         // assume this is a hash function
//!         self.encode().to_vec()
//!     }
//! }
//!
//! impl Transaction {
//!     fn new(amount: Amount) -> Self {
//!         Self {
//!             from: vec![],
//!             to: vec![],
//!             amount,
//!         }
//!     }
//! }
//!
//! let (sender, reveiver) = channel();
//!
//! thread::spawn(move || {
//!     loop {
//!         for tx in reveiver.iter() {
//!             transaction_pre_check(tx);
//!         }
//!     }
//! });
//!
//! (0..10).for_each(|i| sender.send(Transaction::new(i)).unwrap());
//!
//! sleep_ms!(60);
//!
//! begin_block();
//! transaction_formal_check_all();
//! end_block();
//! ```

pub mod mapx;
pub mod mapx_ord;
pub mod mapx_ord_rawkey;
pub mod mapx_raw;
pub mod orphan;
pub mod vecx;

use crate::{
    basic::{
        mapx::Mapx, mapx_ord::MapxOrd, mapx_ord_rawkey::MapxOrdRawKey,
        mapx_ord_rawvalue::MapxOrdRawValue, mapx_raw::MapxRaw, orphan::Orphan,
        vecx::Vecx, vecx_raw::VecxRaw,
    },
    merkle::{MerkleTree, MerkleTreeStore, Proof, ProofEntry},
    BranchName, ParentBranchName, VersionName,
};
use primitive_types::{H128, H160, H256, H512, U128, U256, U512};
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

    /// Check if a version is directly created on the default branch.
    fn version_created(&self, version_name: VersionName) -> bool;

    /// Check if a version is directly created on a specified branch(exclude its parents).
    fn version_created_on_branch(
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

    /// Create a new branch based on the head of the default branch.
    fn branch_create(&self, branch_name: BranchName) -> Result<()>;

    /// Create a new branch based on the head of a specified branch.
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
    ) -> Result<()>;

    /// Create a new branch based on a specified version of a specified branch.
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()>;

    /// Check if a branch exists or not.
    fn branch_exists(&self, branch_name: BranchName) -> bool;

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

    /// Merge a branch to its parent branch.
    ///
    /// NOTE: the original branch will be deleted.
    fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()>;

    /// Check if a branch has children branches.
    fn branch_has_children(&self, branch_name: BranchName) -> bool;

    /// Make a branch to be default,
    /// all default operations will be applied to it.
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()>;

    /// Clean outdated versions out of the default branch.
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()>;

    /// Clean outdated versions out of a specified branch.
    fn prune_by_branch(
        &self,
        branch_name: BranchName,
        reserved_ver_num: Option<usize>,
    ) -> Result<()>;
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

        /// Check if a version is directly created on the default branch.
        #[inline(always)]
        fn version_created(&self, version_name: VersionName) -> bool {
            self.inner.version_created(version_name)
        }

        /// Check if a version is directly created on a specified branch(exclude its parents).
        #[inline(always)]
        fn version_created_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            self.inner
                .version_created_on_branch(version_name, branch_name)
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

        /// Create a new branch based on the head of the default branch.
        #[inline(always)]
        fn branch_create(&self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_create(branch_name).c(d!())
        }

        /// Create a new branch based on the head of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch(
            &self,
            branch_name: BranchName,
            base_branch_name: ParentBranchName,
        ) -> Result<()> {
            self.inner
                .branch_create_by_base_branch(branch_name, base_branch_name)
                .c(d!())
        }

        /// Create a new branch based on a specified version of a specified branch.
        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &self,
            branch_name: BranchName,
            base_branch_name: ParentBranchName,
            base_version_name: VersionName,
        ) -> Result<()> {
            self.inner
                .branch_create_by_base_branch_version(
                    branch_name,
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

        /// Merge a branch to its parent branch.
        #[inline(always)]
        fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()> {
            self.inner.branch_merge_to_parent(branch_name).c(d!())
        }

        /// Check if a branch has children branches.
        #[inline(always)]
        fn branch_has_children(&self, branch_name: BranchName) -> bool {
            self.inner.branch_has_children(branch_name)
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

        /// Clean outdated versions out of a specified reserved number.
        #[inline(always)]
        fn prune_by_branch(
            &self,
            branch_name: BranchName,
            reserved_ver_num: Option<usize>,
        ) -> Result<()> {
            self.inner
                .prune_by_branch(branch_name, reserved_ver_num)
                .c(d!())
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
        fn version_created(&self, _: VersionName) -> bool {
            true
        }

        #[inline(always)]
        fn version_created_on_branch(&self, _: VersionName, _: BranchName) -> bool {
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
        fn branch_create(&self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch(
            &self,
            _: BranchName,
            _: ParentBranchName,
        ) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &self,
            _: BranchName,
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
        fn branch_merge_to_parent(&self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn branch_has_children(&self, _: BranchName) -> bool {
            true
        }

        #[inline(always)]
        fn branch_set_default(&mut self, _: BranchName) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn prune(&self, _: Option<usize>) -> Result<()> {
            Ok(())
        }

        #[inline(always)]
        fn prune_by_branch(&self, _: BranchName, __: Option<usize>) -> Result<()> {
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

impl VsMgmt for MerkleTree {
    impl_vs_methods_nope!();
}

impl VsMgmt for MerkleTreeStore {
    impl_vs_methods_nope!();
}

impl<'a> VsMgmt for Proof<'a> {
    impl_vs_methods_nope!();
}

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
    U128,
    U256,
    U512,
    H128,
    H160,
    H256,
    H512
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
            alt!(!i.version_exists(version_name), return false);
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
            alt!(
                !i.version_exists_on_branch(version_name, branch_name),
                return false
            );
        }
        true
    }

    #[inline(always)]
    fn version_created(&self, version_name: VersionName) -> bool {
        if let Some(i) = self.as_ref() {
            alt!(!i.version_created(version_name), return false);
        }
        true
    }

    #[inline(always)]
    fn version_created_on_branch(
        &self,
        version_name: VersionName,
        branch_name: BranchName,
    ) -> bool {
        if let Some(i) = self.as_ref() {
            alt!(
                !i.version_created_on_branch(version_name, branch_name),
                return false
            );
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
    fn branch_create(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create_by_base_branch(branch_name, base_branch_name)
                .c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_create_by_base_branch_version(
        &self,
        branch_name: BranchName,
        base_branch_name: ParentBranchName,
        base_version_name: VersionName,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_create_by_base_branch_version(
                branch_name,
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
            alt!(!i.branch_exists(branch_name), return false);
        }
        true
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
    fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.branch_merge_to_parent(branch_name).c(d!())?;
        }
        Ok(())
    }

    #[inline(always)]
    fn branch_has_children(&self, branch_name: BranchName) -> bool {
        if let Some(i) = self.as_ref() {
            alt!(!i.branch_has_children(branch_name), return false);
        }
        true
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

    #[inline(always)]
    fn prune_by_branch(
        &self,
        branch_name: BranchName,
        reserved_ver_num: Option<usize>,
    ) -> Result<()> {
        if let Some(i) = self.as_ref() {
            i.prune_by_branch(branch_name, reserved_ver_num).c(d!())?;
        }
        Ok(())
    }
}

/// A helper for implementing `VsMgmt` for collection types,
/// `struct NewType(HashMap)`, `struct NewType(BTreeMap)`, etc.
#[macro_export]
macro_rules! impl_for_collections {
    ($values: tt, $values_mut: tt) => {
        fn version_create(&self, version_name: VersionName) -> Result<()> {
            for i in self.$values() {
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
            for i in self.$values() {
                i.version_create_by_branch(version_name, branch_name)
                    .c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn version_exists(&self, version_name: VersionName) -> bool {
            for i in self.$values() {
                alt!(!i.version_exists(version_name), return false);
            }
            true
        }

        #[inline(always)]
        fn version_exists_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            for i in self.$values() {
                alt!(
                    !i.version_exists_on_branch(version_name, branch_name),
                    return false
                );
            }
            true
        }

        #[inline(always)]
        fn version_created(&self, version_name: VersionName) -> bool {
            for i in self.$values() {
                alt!(!i.version_created(version_name), return false);
            }
            true
        }

        #[inline(always)]
        fn version_created_on_branch(
            &self,
            version_name: VersionName,
            branch_name: BranchName,
        ) -> bool {
            for i in self.$values() {
                alt!(
                    !i.version_created_on_branch(version_name, branch_name),
                    return false
                );
            }
            true
        }

        #[inline(always)]
        fn version_pop(&self) -> Result<()> {
            for i in self.$values() {
                i.version_pop().c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn version_pop_by_branch(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
                i.version_pop_by_branch(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_create(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
                i.branch_create(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch(
            &self,
            branch_name: BranchName,
            base_branch_name: ParentBranchName,
        ) -> Result<()> {
            for i in self.$values() {
                i.branch_create_by_base_branch(branch_name, base_branch_name)
                    .c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_create_by_base_branch_version(
            &self,
            branch_name: BranchName,
            base_branch_name: ParentBranchName,
            base_version_name: VersionName,
        ) -> Result<()> {
            for i in self.$values() {
                i.branch_create_by_base_branch_version(
                    branch_name,
                    base_branch_name,
                    base_version_name,
                )
                .c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_exists(&self, branch_name: BranchName) -> bool {
            for i in self.$values() {
                alt!(!i.branch_exists(branch_name), return false);
            }
            true
        }

        #[inline(always)]
        fn branch_remove(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
                i.branch_remove(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_truncate(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
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
            for i in self.$values() {
                i.branch_truncate_to(branch_name, last_version_name)
                    .c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_pop_version(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
                i.branch_pop_version(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()> {
            for i in self.$values() {
                i.branch_merge_to_parent(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn branch_has_children(&self, branch_name: BranchName) -> bool {
            for i in self.$values() {
                alt!(!i.branch_has_children(branch_name), return false);
            }
            true
        }

        #[inline(always)]
        fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()> {
            for i in self.$values_mut() {
                i.branch_set_default(branch_name).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
            for i in self.$values() {
                i.prune(reserved_ver_num).c(d!())?;
            }
            Ok(())
        }

        #[inline(always)]
        fn prune_by_branch(
            &self,
            branch_name: BranchName,
            reserved_ver_num: Option<usize>,
        ) -> Result<()> {
            for i in self.$values() {
                i.prune_by_branch(branch_name, reserved_ver_num).c(d!())?;
            }
            Ok(())
        }
    };
}
