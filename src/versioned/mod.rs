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
//! use vsdb::{BranchName, MapxVs, OrphanVs, VecxVs, VersionName};
//!
//! type Amount = u64;
//! type Address = Vec<u8>;
//! type ConsensusInt = i128;
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
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
//!         snap0.pop_version().unwrap();
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
//!             snap1.pop_version().unwrap();
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
//!         if !ws.branch_exists(MASTER_BRANCH) {
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
//!     fn apply_transaction(&self, tx: &Transaction) -> Result<()> {
//!         self.a_very_complex_function_will_change_state(tx).c(d!())
//!     }
//!
//!     // sample code
//!     fn a_very_complex_function_will_change_state(
//!         &self,
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
//!     fn branch_exists(&self, branch: &str) -> bool {
//!         let br = BranchName(branch.as_bytes());
//!         self.transactions.branch_exists(br)
//!             && self.balances.branch_exists(br)
//!             && self.a_consensus_int.branch_exists(br)
//!     }
//!
//!     fn new_branch(&self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.transactions
//!             .branch_create(br)
//!             .c(d!())
//!             .and_then(|_| self.balances.branch_create(br).c(d!()))
//!             .and_then(|_| self.a_consensus_int.branch_create(br).c(d!()))
//!     }
//!
//!     fn delete_branch(&self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.transactions
//!             .branch_remove(br)
//!             .c(d!())
//!             .and_then(|_| self.balances.branch_remove(br).c(d!()))
//!             .and_then(|_| self.a_consensus_int.branch_remove(br).c(d!()))
//!     }
//!
//!     fn merge_branch(&self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.transactions
//!             .branch_merge_to_parent(br)
//!             .c(d!())
//!             .and_then(|_| self.balances.branch_merge_to_parent(br).c(d!()))
//!             .and_then(|_| self.a_consensus_int.branch_merge_to_parent(br).c(d!()))
//!     }
//!
//!     fn set_default_branch(&self, branch: &str) -> Result<()> {
//!         let br = BranchName(branch.as_bytes());
//!         self.transactions
//!             .branch_set_default(br)
//!             .c(d!())
//!             .and_then(|_| self.balances.branch_set_default(br).c(d!()))
//!             .and_then(|_| self.a_consensus_int.branch_set_default(br).c(d!()))
//!     }
//!
//!     fn reset_branch(&self, branch: &str) -> Result<()> {
//!         self.set_default_branch(MASTER_BRANCH)
//!             .c(d!())
//!             .and_then(|_| self.delete_branch(branch).c(d!()))
//!             .and_then(|_| self.new_branch(branch).c(d!()))
//!             .and_then(|_| self.set_default_branch(branch).c(d!()))
//!     }
//!
//!     fn push_version(&self, version: &[u8]) -> Result<()> {
//!         let ver = VersionName(version);
//!         self.transactions
//!             .version_create(ver)
//!             .c(d!())
//!             .and_then(|_| self.balances.version_create(ver).c(d!()))
//!             .and_then(|_| self.a_consensus_int.version_create(ver).c(d!()))
//!     }
//!
//!     fn pop_version(&self) -> Result<()> {
//!         self.transactions
//!             .version_pop()
//!             .c(d!())
//!             .and_then(|_| self.balances.version_pop().c(d!()))
//!             .and_then(|_| self.a_consensus_int.version_pop().c(d!()))
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
//!         bcs::to_bytes(self).unwrap()
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
//! sleep_ms!(200);
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

use crate::{BranchName, ParentBranchName, VersionName};
use ruc::*;

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
    fn branch_merge_to_parent(&self, branch_name: BranchName) -> Result<()>;

    /// Check if a branch has children branches.
    fn branch_has_children(&self, branch_name: BranchName) -> bool;

    /// Make a branch to be default,
    /// all default operations will be applied to it.
    fn branch_set_default(&mut self, branch_name: BranchName) -> Result<()>;

    /// Clean outdated versions out of the default reserved number.
    fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()>;

    /// Clean outdated versions out of a specified reserved number.
    fn prune_by_branch(
        &self,
        branch_name: BranchName,
        reserved_ver_num: Option<usize>,
    ) -> Result<()>;
}
