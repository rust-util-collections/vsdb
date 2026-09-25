//! Versioned, self-contained proof transport; independent of trie cache files.

use super::{MptProof, SmtProof};
use crate::{Result, VsdbError};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct,
};
use std::result::Result as StdResult;

// These tags and field orders are wire contracts. A new layout needs a new tag.
const MPT_FORMAT: [u8; 8] = *b"VSMPTP01";
const SMT_FORMAT: [u8; 8] = *b"VSSMTP01";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MptWire {
    format: [u8; 8],
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    nodes: Vec<Vec<u8>>,
}

impl Serialize for MptProof {
    fn serialize<S: Serializer>(&self, serializer: S) -> StdResult<S::Ok, S::Error> {
        let mut wire = serializer.serialize_struct("MptProof", 4)?;
        wire.serialize_field("format", &MPT_FORMAT)?;
        wire.serialize_field("key", &self.key)?;
        wire.serialize_field("value", &self.value)?;
        wire.serialize_field("nodes", &self.nodes)?;
        wire.end()
    }
}

impl<'de> Deserialize<'de> for MptProof {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
        let wire = MptWire::deserialize(deserializer)?;
        if wire.format != MPT_FORMAT {
            return Err(de::Error::custom("unsupported MPT proof format"));
        }
        Ok(Self {
            key: wire.key,
            value: wire.value,
            nodes: wire.nodes,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SmtWire {
    format: [u8; 8],
    key_hash: [u8; 32],
    leaf: Option<([u8; 32], Vec<u8>)>,
    siblings: Vec<[u8; 32]>,
}

impl Serialize for SmtProof {
    fn serialize<S: Serializer>(&self, serializer: S) -> StdResult<S::Ok, S::Error> {
        let mut wire = serializer.serialize_struct("SmtProof", 4)?;
        wire.serialize_field("format", &SMT_FORMAT)?;
        wire.serialize_field("key_hash", &self.key_hash)?;
        wire.serialize_field("leaf", &self.leaf)?;
        wire.serialize_field("siblings", &self.siblings)?;
        wire.end()
    }
}

impl<'de> Deserialize<'de> for SmtProof {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
        let wire = SmtWire::deserialize(deserializer)?;
        if wire.format != SMT_FORMAT {
            return Err(de::Error::custom("unsupported SMT proof format"));
        }
        Ok(Self {
            key_hash: wire.key_hash,
            leaf: wire.leaf,
            siblings: wire.siblings,
        })
    }
}

fn decode<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (proof, remaining) = postcard::take_from_bytes(bytes)?;
    if !remaining.is_empty() {
        return Err(VsdbError::Decode {
            detail: "trailing bytes after proof".into(),
        });
    }
    Ok(proof)
}

impl MptProof {
    /// Encodes this proof as the version-1 postcard representation.
    ///
    /// Fields, in order: the eight-byte `VSMPTP01` tag, key, optional value,
    /// and encoded nodes. This is also the representation used by serde.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Decodes exactly one versioned proof, rejecting trailing bytes.
    ///
    /// Checks the transport format, not whether the proof matches a root.
    /// Call [`MptCalc::verify_proof`](super::MptCalc::verify_proof) with the
    /// expected root and key before relying on the decoded value.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
}

impl SmtProof {
    /// Encodes this proof as the version-1 postcard representation.
    ///
    /// Fields, in order: the eight-byte `VSSMTP01` tag, key hash, optional
    /// terminal leaf, and sibling hashes. Serde uses the same representation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Decodes exactly one versioned proof, rejecting trailing bytes.
    ///
    /// Checks the transport format, not whether the proof matches a root.
    /// Call [`SmtCalc::verify_proof`](super::SmtCalc::verify_proof) with the
    /// expected root and key before relying on the decoded value.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
}
