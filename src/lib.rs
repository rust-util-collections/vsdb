//!
//! # An mixed(mem&disk) cache implementation
//!

#![deny(warnings)]
#![deny(missing_docs)]

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
/// Alias for compatible purpose
#[cfg(not(feature = "diskcache"))]
pub type Mapx<K, V> = std::collections::HashMap<K, V>;

#[cfg(feature = "diskcache")]
pub use vecx::Vecx;
/// Alias for compatible purpose
#[cfg(not(feature = "diskcache"))]
pub type Vecx<T> = Vec<T>;

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
            "{}/__extra_meta/{}/{}_{}_{}_{}",
            *$crate::helper::DATA_DIR,
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
        #[cfg(feature = "diskcache")]
        {
            let obj: $crate::Vecx<$ty> = $crate::try_twice!($crate::Vecx::new(&$crate::unique_path!()))
            obj
        }
        #[cfg(not(feature = "diskcache"))]
        {
            Vec::new()
        }
    }};
    ($path: expr) => {{
        #[cfg(feature = "diskcache")]
        {
            $crate::try_twice!($crate::Vecx::new($path))
        }
        #[cfg(not(feature = "diskcache"))]
        {
            let _ = $path;
            Vec::new()
        }
    }};
    () => {{
        #[cfg(feature = "diskcache")]
        {
            $crate::try_twice!($crate::Vecx::new(&$crate::unique_path!()))
        }
        #[cfg(not(feature = "diskcache"))]
        {
            Vec::new()
        }
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
        #[cfg(feature = "diskcache")]
        {
            let obj: $crate::Mapx<$ty> =
                $crate::try_twice!($crate::Mapx::new(&$crate::unique_path!()));
            obj
        }
        #[cfg(not(feature = "diskcache"))]
        {
            std::collections::HashMap::new()
        }
    }};
    ($path: expr) => {{
        #[cfg(feature = "diskcache")]
        {
            $crate::try_twice!($crate::Mapx::new(&*$path))
        }
        #[cfg(not(feature = "diskcache"))]
        {
            let _ = $path;
            std::collections::HashMap::new()
        }
    }};
    () => {{
        #[cfg(feature = "diskcache")]
        {
            $crate::try_twice!($crate::Mapx::new(&$crate::unique_path!()))
        }
        #[cfg(not(feature = "diskcache"))]
        {
            std::collections::HashMap::new()
        }
    }};
}
