//!
//! # An mixed(mem&disk) cache implementation
//!

#![deny(warnings)]
#![deny(missing_docs)]

///////////////////////////////////////

#[cfg(feature = "diskcache")]
pub mod helper;
#[cfg(feature = "diskcache")]
pub mod mapx;
#[cfg(feature = "diskcache")]
mod serde;
#[cfg(feature = "diskcache")]
pub mod vecx;

#[cfg(feature = "diskcache")]
pub use mapx::Mapx;
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
pub use veci::Veci as Vecx;

///////////////////////////////////////

use lazy_static::lazy_static;
use std::env;

/// Flush data to disk
#[inline(always)]
pub fn flush_data() {
    #[cfg(feature = "diskcache")]
    helper::BNC.flush().unwrap();
}

/// Delete all KVs
pub fn clear() {
    #[cfg(feature = "diskcache")]
    helper::rocksdb_clear();
}

#[inline(always)]
fn gen_data_dir() -> String {
    // Is it necessary to be compatible with Windows OS?
    let d = env::var("BNC_DATA_DIR").unwrap_or_else(|_| "/tmp/.bnc".to_owned());
    std::fs::create_dir_all(&d).unwrap();
    d
}

lazy_static! {
    #[allow(missing_docs)]
    pub static ref DATA_DIR: String = gen_data_dir();
}

/// meta of each instance, Vecx/Mapx, etc.
pub const BNC_META_NAME: &str = "__extra_meta__";

/// Try once more when we fail to open a db.
#[macro_export]
macro_rules! try_twice {
    ($ops: expr) => {
        ruc::pnk!($ops.c(d!()).or_else(|e| {
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
            "{}/{}/{}/{}_{}_{}_{}",
            *$crate::DATA_DIR,
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
            $crate::try_twice!($crate::Vecx::new(&format!("{}/{}/{}", *$crate::DATA_DIR, $crate::BNC_META_NAME, &*$path)))
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
            "{}/{}/{}",
            *$crate::DATA_DIR,
            $crate::BNC_META_NAME,
            &*$path
        )))
    }};
    () => {{
        $crate::try_twice!($crate::Mapx::new(&$crate::unique_path!()))
    }};
}
