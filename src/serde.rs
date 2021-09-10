//!
//! # Serde Vistor Implementation
//!
//! Used to restore an existing database.
//!

use serde::{Deserialize, Serialize};

pub(crate) struct CacheVisitor;

impl<'de> serde::de::Visitor<'de> for CacheVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut ::core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("The fucking world is over!")
    }

    fn visit_str<E>(self, v: &str) -> core::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.to_owned())
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct CacheMeta<'a> {
    pub in_mem_cnt: usize,
    pub root_path: &'a str,
}
