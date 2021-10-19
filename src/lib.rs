//!
//! # An mixed(mem&disk) cache implementation
//!

#![deny(warnings)]
#![deny(missing_docs)]

///////////////////////////////////////

#[cfg(feature = "diskcache")]
mod helper;
#[cfg(feature = "diskcache")]
pub mod mapx;
#[cfg(feature = "diskcache")]
pub mod mapxnk;
#[cfg(feature = "diskcache")]
mod serde;
#[cfg(feature = "diskcache")]
pub mod vecx;

#[cfg(feature = "diskcache")]
pub use mapx::Mapx;
#[cfg(feature = "diskcache")]
pub use mapxnk::Mapxnk;
#[cfg(feature = "diskcache")]
pub use vecx::Vecx;

///////////////////////////////////////

#[cfg(not(feature = "diskcache"))]
pub mod mapi;
#[cfg(not(feature = "diskcache"))]
pub mod veci;

#[cfg(not(feature = "diskcache"))]
pub use mapi::Mapi as Mapx;
#[cfg(not(feature = "diskcache"))]
pub use mapi::Mapi as Mapxnk;
#[cfg(not(feature = "diskcache"))]
pub use veci::Veci as Vecx;

///////////////////////////////////////

use lazy_static::lazy_static;
use ruc::*;
use std::{
    env, ptr,
    sync::atomic::{AtomicBool, Ordering},
};

lazy_static! {
    static ref BNC_DATA_DIR: String = gen_data_dir();
    #[allow(missing_docs)]
    pub static ref BNC_DATA_LIST: Vec<String> =
        (0..DB_NUM).map(|i| format!("{}/{}", &*BNC_DATA_DIR, i)).collect();
}

const DB_NUM: usize = 8;

/// meta of each instance, Vecx/Mapx, etc.
pub const BNC_META_NAME: &str = "__extra_meta__";

static DATA_DIR: String = String::new();

#[inline(always)]
fn gen_data_dir() -> String {
    let d = if DATA_DIR.is_empty() {
        // Is it necessary to be compatible with Windows OS?
        env::var("BNC_DATA_DIR").unwrap_or_else(|_| "/tmp/.bnc".to_owned())
    } else {
        DATA_DIR.clone()
    };
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Set ${BNC_DATA_DIR} manually
pub fn set_data_dir(dir: &str) -> Result<()> {
    lazy_static! {
        static ref HAS_INITED: AtomicBool = AtomicBool::new(false);
    }

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("BNC has been initialized !!"))
    } else {
        unsafe {
            ptr::swap(DATA_DIR.as_ptr() as *mut u8, dir.to_owned().as_mut_ptr());
        }
        Ok(())
    }
}

/// Delete all KVs
pub fn clear() {
    #[cfg(feature = "diskcache")]
    helper::rocksdb_clear();
}

/// Flush data to disk
#[inline(always)]
pub fn flush_data() {
    #[cfg(feature = "diskcache")]
    (0..DB_NUM).for_each(|i| {
        helper::BNC[i].flush().unwrap();
    });
}

/// Try once more when we fail to open a db.
#[macro_export]
macro_rules! try_twice {
    ($ops: expr) => {
        pnk!($ops.c(d!()).or_else(|e| {
            e.print(None);
            $ops.c(d!())
        }))
    };
}

/// Generate a unique path for each instance.
#[macro_export]
macro_rules! unique_path {
    () => {
        format!(
            "{}/{}/{}_{}_{}_{}",
            $crate::BNC_META_NAME,
            ts!(),
            file!(),
            line!(),
            column!(),
            rand::random::<u32>()
        )
    };
}

/// A helper for creating Vecx.
#[macro_export]
macro_rules! new_vecx {
    (@$ty: ty) => {
        $crate::new_vecx_custom!($ty)
    };
    ($path:expr) => {
        $crate::new_vecx_custom!($path)
    };
    () => {
        $crate::new_vecx_custom!()
    };
}

/// A helper for creating Vecx.
#[macro_export]
macro_rules! new_vecx_custom {
    (@$ty: ty) => {{
            let obj: $crate::Vecx<$ty> = $crate::try_twice!($crate::Vecx::new(&$crate::unique_path!()))
            obj
    }};
    ($path: expr) => {{
            $crate::try_twice!($crate::Vecx::new(&format!("{}/{}", $crate::BNC_META_NAME, &*$path)))
    }};
    () => {{
            $crate::try_twice!($crate::Vecx::new(&$crate::unique_path!()))
    }};
}

/// A helper for creating Mapx.
#[macro_export]
macro_rules! new_mapx {
    (@$ty: ty) => {
        $crate::new_mapx_custom!($ty)
    };
    ($path:expr) => {
        $crate::new_mapx_custom!($path)
    };
    () => {
        $crate::new_mapx_custom!()
    };
}

/// A helper for creating Mapx.
#[macro_export]
macro_rules! new_mapx_custom {
    (@$ty: ty) => {{
        let obj: $crate::Mapx<$ty> =
            $crate::try_twice!($crate::Mapx::new(&$crate::unique_path!()));
        obj
    }};
    ($path: expr) => {{
        $crate::try_twice!($crate::Mapx::new(&format!(
            "{}/{}",
            $crate::BNC_META_NAME,
            &*$path
        )))
    }};
    () => {{
        $crate::try_twice!($crate::Mapx::new(&$crate::unique_path!()))
    }};
}

/// A helper for creating Mapxnk.
#[macro_export]
macro_rules! new_mapxnk {
    (@$ty: ty) => {
        $crate::new_mapxnk_custom!($ty)
    };
    ($path:expr) => {
        $crate::new_mapxnk_custom!($path)
    };
    () => {
        $crate::new_mapxnk_custom!()
    };
}

/// A helper for creating Mapxnk.
#[macro_export]
macro_rules! new_mapxnk_custom {
    (@$ty: ty) => {{
        let obj: $crate::Mapxnk<$ty> =
            $crate::try_twice!($crate::Mapxnk::new(&$crate::unique_path!()));
        obj
    }};
    ($path: expr) => {{
        $crate::try_twice!($crate::Mapxnk::new(&format!(
            "{}/{}",
            $crate::BNC_META_NAME,
            &*$path
        )))
    }};
    () => {{
        $crate::try_twice!($crate::Mapxnk::new(&$crate::unique_path!()))
    }};
}
