//! Post-order checkpoint records. The decoder retains only completed
//! subtrees and validates each parent before joining it to its children.
use super::{
    Checkpoint, MAX_MPT_KEY_LEN,
    nibbles::Nibbles,
    node::{Children, Node},
    shared::{Handle, TreeNode},
    smt::{BitPath, SmtNode},
};
use crate::{InstanceId, Result, VsdbError, common::atomic_write_file_with};
use sha3::{Digest, Keccak256};
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

pub(super) const MAX_FILE: u64 = 512 << 20;
const MAX_DECODED: usize = 512 << 20;
const MAX_VALUE: usize = 64 << 20;
const MAX_NODES: usize = 2_000_000;
const MAX_PENDING: usize = 15 * 2 * MAX_MPT_KEY_LEN + 1;
const END: u8 = 0xff;

fn invalid(message: &str) -> VsdbError {
    VsdbError::Trie {
        detail: format!("invalid trie checkpoint: {message}"),
    }
}
struct Output<W> {
    writer: W,
    hash: Keccak256,
    left: u64,
}
impl<W: Write> Write for Output<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() as u64 > self.left {
            return Err(io::Error::other("checkpoint exceeds size limit"));
        }
        let n = self.writer.write(data)?;
        self.hash.update(&data[..n]);
        self.left -= n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
pub(super) struct Input<R> {
    reader: R,
    hash: Keccak256,
    left: u64,
    budget: usize,
}
impl<R: Read> Input<R> {
    fn take(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.left = self
            .left
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| invalid("truncated payload"))?;
        self.reader.read_exact(bytes)?;
        self.hash.update(&*bytes);
        Ok(())
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut b = [0; N];
        self.take(&mut b)?;
        Ok(b)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.fixed::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }
    fn reserve(&mut self, n: usize) -> Result<()> {
        self.budget = self
            .budget
            .checked_sub(n)
            .ok_or_else(|| invalid("decoded memory limit"))?;
        Ok(())
    }
    fn value(&mut self) -> Result<Vec<u8>> {
        let len = u32::from_le_bytes(self.fixed()?) as usize;
        if len > MAX_VALUE || len as u64 > self.left {
            return Err(invalid("invalid value length"));
        }
        self.reserve(len)?;
        let mut value = vec![0; len];
        self.take(&mut value)?;
        Ok(value)
    }
    fn packed(&mut self, max: usize, bits_per_byte: usize) -> Result<(usize, Vec<u8>)> {
        let len = self.u16()? as usize;
        if len > max {
            return Err(invalid("oversize path"));
        }
        let count = len.div_ceil(bits_per_byte);
        self.reserve(count)?;
        let mut bytes = vec![0; count];
        self.take(&mut bytes)?;
        let pad = (count * bits_per_byte - len) * (8 / bits_per_byte);
        if bytes.last().is_some_and(|b| *b & ((1 << pad) - 1) != 0) {
            return Err(invalid("nonzero path padding"));
        }
        Ok((len, bytes))
    }
}
fn packed(w: &mut impl Write, len: usize, data: &[u8]) -> Result<()> {
    w.write_all(
        &u16::try_from(len)
            .map_err(|_| invalid("oversize path"))?
            .to_le_bytes(),
    )?;
    w.write_all(data)?;
    Ok(())
}
fn value(w: &mut impl Write, v: &[u8]) -> Result<()> {
    if v.len() > MAX_VALUE {
        return Err(invalid("oversize value"));
    }
    w.write_all(&(v.len() as u32).to_le_bytes())?;
    w.write_all(v)?;
    Ok(())
}
pub(super) trait CacheNode: TreeNode {
    const MAGIC: [u8; 5];
    const NAME: &'static str;
    type Summary;
    /// Variable allocations charged by the loader, excluding the fixed node record.
    fn variable_bytes(&self) -> usize;
    fn write_body(&self, w: &mut impl Write) -> Result<()>;
    fn read_body(
        tag: u8,
        r: &mut Input<impl Read>,
        stack: &mut Vec<(Handle<Self>, Self::Summary)>,
    ) -> Result<(Self, Self::Summary)>;
    fn validate_root(summary: &Self::Summary) -> bool;
}
impl CacheNode for Node {
    const MAGIC: [u8; 5] = *b"MPTC\x02";
    const NAME: &'static str = "mpt";
    // Furthest leaf/value in nibbles. All terminal lengths in this subtree
    // have the same parity, checked when a branch combines its children.
    type Summary = usize;
    fn variable_bytes(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Leaf { path, value } => path.bytes().len() * 2 + value.len(),
            Self::Extension { path, .. } => path.bytes().len() * 2,
            Self::Branch { children, value } => {
                size_of_val(&*children.nodes) + value.as_deref().map_or(0, <[u8]>::len)
            }
        }
    }
    fn write_body(&self, w: &mut impl Write) -> Result<()> {
        match self {
            Self::Null => w.write_all(&[0])?,
            Self::Leaf { path, value: v } => {
                w.write_all(&[1])?;
                packed(w, path.len(), path.bytes())?;
                value(w, v)?
            }
            Self::Extension { path, .. } => {
                w.write_all(&[2])?;
                packed(w, path.len(), path.bytes())?
            }
            Self::Branch { children, value: v } => {
                w.write_all(&[3])?;
                w.write_all(&children.bitmap.to_le_bytes())?;
                w.write_all(&[u8::from(v.is_some())])?;
                if let Some(v) = v {
                    value(w, v)?
                }
            }
        };
        Ok(())
    }
    fn read_body(
        tag: u8,
        r: &mut Input<impl Read>,
        stack: &mut Vec<(Handle<Self>, usize)>,
    ) -> Result<(Self, usize)> {
        let max = 2 * MAX_MPT_KEY_LEN;
        match tag {
            0 => Ok((Self::Null, 0)),
            1 | 2 => {
                let (len, bytes) = r.packed(max, 2)?;
                r.reserve(bytes.len())?;
                let path = Nibbles::packed(len, &bytes);
                if tag == 1 {
                    return Ok((
                        Self::Leaf {
                            path,
                            value: r.value()?,
                        },
                        len,
                    ));
                }
                let (child, depth) = stack
                    .pop()
                    .ok_or_else(|| invalid("missing extension child"))?;
                if len == 0
                    || depth + len > max
                    || !matches!(child.node(), Self::Branch { .. })
                {
                    return Err(invalid("noncanonical extension"));
                }
                Ok((Self::Extension { path, child }, len + depth))
            }
            3 => {
                let bitmap = r.u16()?;
                let count = bitmap.count_ones() as usize;
                let flag = r.u8()?;
                if count == 0
                    || flag > 1
                    || count + usize::from(flag) < 2
                    || count > stack.len()
                {
                    return Err(invalid("invalid branch"));
                }
                let v = if flag == 1 {
                    Some(r.value()?.into_boxed_slice())
                } else {
                    None
                };
                r.reserve(count * size_of::<Handle<Node>>())?;
                let mut nodes = Vec::with_capacity(count);
                let mut depth = 0;
                let mut parity = v.as_ref().map(|_| 0);
                for (node, d) in stack.drain(stack.len() - count..) {
                    if matches!(node.node(), Node::Null)
                        || d + 1 > max
                        || parity.is_some_and(|p| p != (d + 1) % 2)
                    {
                        return Err(invalid("invalid branch child"));
                    }
                    depth = depth.max(d + 1);
                    parity = Some((d + 1) % 2);
                    nodes.push(node);
                }
                // Children were visited last-to-first by the post-order stack.
                nodes.reverse();
                Ok((
                    Self::Branch {
                        children: Children {
                            bitmap,
                            nodes: nodes.into_boxed_slice(),
                        },
                        value: v,
                    },
                    depth,
                ))
            }
            _ => Err(invalid("unknown MPT node")),
        }
    }
    fn validate_root(depth: &usize) -> bool {
        depth.is_multiple_of(2)
    }
}
impl CacheNode for SmtNode {
    const MAGIC: [u8; 5] = *b"SMTC\x04";
    const NAME: &'static str = "smt";
    // Starting bit offset and representative leaf. Children already proved
    // that all their leaves agree on earlier ancestor bits.
    type Summary = (usize, [u8; 32]);
    fn variable_bytes(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Leaf { path, value, .. } => path.as_packed().len() + value.len(),
            Self::Internal { path, .. } => path.as_packed().len(),
        }
    }
    fn write_body(&self, w: &mut impl Write) -> Result<()> {
        match self {
            Self::Empty => w.write_all(&[0])?,
            Self::Leaf {
                path,
                key_hash,
                value: v,
            } => {
                w.write_all(&[1])?;
                packed(w, path.len(), path.as_packed())?;
                w.write_all(key_hash)?;
                value(w, v)?
            }
            Self::Internal { path, .. } => {
                w.write_all(&[2])?;
                packed(w, path.len(), path.as_packed())?
            }
        };
        Ok(())
    }
    fn read_body(
        tag: u8,
        r: &mut Input<impl Read>,
        stack: &mut Vec<(Handle<Self>, Self::Summary)>,
    ) -> Result<(Self, Self::Summary)> {
        if tag == 0 {
            return Ok((Self::Empty, (0, [0; 32])));
        }
        let (len, bytes) = r.packed(256, 8)?;
        let path = BitPath::from_packed(&bytes, len);
        match tag {
            1 => {
                let key = r.fixed()?;
                let start = 256 - len;
                if !BitPath::from_hash(&key).starts_with_from(start, &path) {
                    return Err(invalid("SMT leaf path disagrees with key"));
                }
                Ok((
                    Self::Leaf {
                        path,
                        key_hash: key,
                        value: r.value()?,
                    },
                    (start, key),
                ))
            }
            2 => {
                let (left, (ld, lk)) =
                    stack.pop().ok_or_else(|| invalid("missing left child"))?;
                let (right, (rd, rk)) =
                    stack.pop().ok_or_else(|| invalid("missing right child"))?;
                if ld != rd || ld <= len {
                    return Err(invalid("inconsistent SMT depths"));
                }
                let start = ld - len - 1;
                let l = BitPath::from_hash(&lk);
                let rr = BitPath::from_hash(&rk);
                if l.common_prefix(&rr) != ld - 1
                    || l.bit_at(ld - 1) != 0
                    || !l.starts_with_from(start, &path)
                {
                    return Err(invalid("SMT child outside parent path"));
                }
                Ok((Self::Internal { path, left, right }, (start, lk)))
            }
            _ => Err(invalid("unknown SMT node")),
        }
    }
    fn validate_root((depth, _): &Self::Summary) -> bool {
        *depth == 0
    }
}
pub(super) fn save<N: CacheNode>(
    path: &Path,
    root: &Handle<N>,
    id: Checkpoint,
    limit: u64,
) -> Result<()> {
    atomic_write_file_with(path, |file| {
        let mut out = Output {
            writer: BufWriter::new(file),
            hash: Keccak256::new(),
            left: limit
                .min(MAX_FILE)
                .checked_sub(8)
                .ok_or_else(|| invalid("size limit"))?,
        };
        out.write_all(&N::MAGIC)?;
        out.write_all(&id.universe.to_le_bytes())?;
        for n in [
            id.map.map_id,
            id.map.ns.unwrap_or_default(),
            id.commit,
            id.tree_root,
        ] {
            out.write_all(&n.to_le_bytes())?;
        }
        out.write_all(&root.root_hash())?;
        let mut work = vec![(root, false)];
        let mut count = 0;
        let mut remaining_budget = MAX_DECODED;
        while let Some((node, visited)) = work.pop() {
            if visited {
                node.node().write_body(&mut out)?;
            } else {
                count += 1;
                remaining_budget = remaining_budget
                    .checked_sub(
                        node.node().variable_bytes()
                            + size_of::<N>()
                            + size_of::<N::Summary>()
                            + 96,
                    )
                    .ok_or_else(|| invalid("decoded memory limit"))?;
                if count > MAX_NODES {
                    return Err(invalid("node limit"));
                }
                work.push((node, true));
                node.node().children(|ch| work.push((ch, false)));
            }
        }
        out.write_all(&[END])?;
        out.writer.write_all(&out.hash.finalize()[..8])?;
        out.writer.flush()?;
        Ok(())
    })
}
pub(super) fn load<N: CacheNode>(path: &Path) -> Result<(Handle<N>, Checkpoint)> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    if !(8..=MAX_FILE).contains(&len) {
        return Err(invalid("size limit"));
    }
    let mut r = Input {
        reader: BufReader::new(file),
        hash: Keccak256::new(),
        left: len - 8,
        budget: MAX_DECODED,
    };
    if r.fixed::<5>()? != N::MAGIC {
        return Err(invalid("version"));
    }
    let universe = u128::from_le_bytes(r.fixed()?);
    let map = InstanceId::new(
        u64::from_le_bytes(r.fixed()?),
        u64::from_le_bytes(r.fixed()?),
    );
    let commit = u64::from_le_bytes(r.fixed()?);
    let tree_root = u64::from_le_bytes(r.fixed()?);
    let expected = r.fixed::<32>()?;
    let mut stack = Vec::new();
    let mut count = 0;
    loop {
        let tag = r.u8()?;
        if tag == END {
            break;
        }
        // Empty/leaf records push without popping any completed children.
        // Refuse an overfull frontier before allocating their path or value.
        if tag <= 1 && stack.len() >= MAX_PENDING {
            return Err(invalid("pending subtree limit"));
        }
        count += 1;
        if count > MAX_NODES {
            return Err(invalid("node limit"));
        }
        r.reserve(size_of::<N>() + size_of::<N::Summary>() + 96)?;
        let (node, summary) = N::read_body(tag, &mut r, &mut stack)?;
        if stack.len() >= MAX_PENDING {
            return Err(invalid("pending subtree limit"));
        }
        let node = Handle::new(node);
        node.root_hash();
        stack.push((node, summary));
    }
    let (node, summary) = stack.pop().ok_or_else(|| invalid("empty checkpoint"))?;
    if !stack.is_empty()
        || r.left != 0
        || !N::validate_root(&summary)
        || node.root_hash() != expected
    {
        return Err(invalid("inconsistent trie root"));
    }
    let mut stored = [0; 8];
    r.reader.read_exact(&mut stored)?;
    if stored != r.hash.finalize()[..8] {
        return Err(invalid("checksum"));
    }
    Ok((
        node,
        Checkpoint {
            universe,
            map,
            commit,
            tree_root,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::{MptCalc, SmtCalc};

    fn reject<N: CacheNode>(template: &[u8], records: &[u8], reason: &str) {
        let dir = super::super::test::scratch_cache_dir();
        let path = dir.join("index");
        let mut wire = template[..85].to_vec();
        wire.extend_from_slice(records);
        wire.push(END);
        let digest = Keccak256::digest(&wire);
        wire.extend_from_slice(&digest[..8]);
        std::fs::write(&path, wire).unwrap();
        assert!(
            load::<N>(&path)
                .err()
                .expect("invalid checkpoint accepted")
                .to_string()
                .contains(reason)
        );
    }

    #[test]
    fn validates_untrusted_postorder_shapes_before_building_parents() {
        let dir = super::super::test::scratch_cache_dir();
        MptCalc::new().save_cache(&dir, 1, 0).unwrap();
        SmtCalc::new().save_cache(&dir, 1, 0).unwrap();
        let mpt = std::fs::read(dir.join("mpt_cache_1.bin")).unwrap();
        let smt = std::fs::read(dir.join("smt_cache_1.bin")).unwrap();
        reject::<Node>(&mpt, &[1, 1, 0, 0x0f], "padding");
        reject::<Node>(&mpt, &[1, 0, 0, 255, 255, 255, 255], "value length");
        reject::<Node>(
            &mpt,
            &[1, 0, 0, 0, 0, 0, 0, 2, 1, 0, 0],
            "noncanonical extension",
        );
        reject::<Node>(&mpt, &[3, 1, 0, 0], "invalid branch");
        reject::<Node>(&mpt, &[1, 1, 8], "oversize path");
        reject::<Node>(&mpt, &vec![0; MAX_PENDING + 1], "pending subtree limit");
        let mut incoherent_leaf = vec![1, 1, 0, 128];
        incoherent_leaf.extend([0; 32]);
        reject::<SmtNode>(&smt, &incoherent_leaf, "disagrees with key");
        reject::<SmtNode>(&smt, &[0, 0, 2, 0, 0], "inconsistent SMT depths");
        // Two leaf suffixes of length 255 have compatible depths but share
        // the same leading bit; a binary parent cannot own both.
        let mut leaves = Vec::new();
        for _ in 0..2 {
            leaves.extend([1, 255, 0]);
            leaves.extend([0; 32]);
            leaves.extend([0; 32]);
            leaves.extend([0; 4]);
        }
        leaves.extend([2, 0, 0]);
        reject::<SmtNode>(&smt, &leaves, "outside parent path");
    }
}
