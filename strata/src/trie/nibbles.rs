//! Packed nibble paths. A 32-byte key fits entirely inside its node.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Bytes {
    Inline([u8; 32]),
    Heap(Box<[u8]>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Nibbles {
    len: usize,
    bytes: Bytes,
}
impl Default for Nibbles {
    fn default() -> Self {
        Self::from_raw(&[])
    }
}
impl Nibbles {
    pub fn from_raw(raw: &[u8]) -> Self {
        Self::packed(raw.len() * 2, raw)
    }
    pub fn packed(len: usize, raw: &[u8]) -> Self {
        let bytes = if raw.len() <= 32 {
            let mut buf = [0; 32];
            buf[..raw.len()].copy_from_slice(raw);
            Bytes::Inline(buf)
        } else {
            Bytes::Heap(raw.into())
        };
        Self { len, bytes }
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn bytes(&self) -> &[u8] {
        match &self.bytes {
            Bytes::Inline(b) => &b[..self.len.div_ceil(2)],
            Bytes::Heap(b) => b,
        }
    }
    pub fn at(&self, i: usize) -> u8 {
        let b = self.bytes()[i / 2];
        if i.is_multiple_of(2) { b >> 4 } else { b & 15 }
    }
    pub fn matches_at(&self, offset: usize, part: &Self) -> bool {
        self.len.saturating_sub(offset) >= part.len
            && (0..part.len).all(|i| self.at(offset + i) == part.at(i))
    }
    pub fn common_prefix(&self, other: &Self) -> usize {
        (0..self.len.min(other.len))
            .take_while(|&i| self.at(i) == other.at(i))
            .count()
    }
    fn build(len: usize, digit: impl Fn(usize) -> u8) -> Self {
        let mut result = Self {
            len,
            bytes: if len <= 64 {
                Bytes::Inline([0; 32])
            } else {
                Bytes::Heap(vec![0; len.div_ceil(2)].into())
            },
        };
        let out = match &mut result.bytes {
            Bytes::Inline(b) => &mut b[..],
            Bytes::Heap(b) => b,
        };
        for (i, b) in out[..len.div_ceil(2)].iter_mut().enumerate() {
            *b = digit(2 * i) << 4 | if 2 * i + 1 < len { digit(2 * i + 1) } else { 0 };
        }
        result
    }
    pub fn slice(&self, start: usize, end: usize) -> Self {
        Self::build(end - start, |i| self.at(start + i))
    }
    pub fn concat(&self, other: &Self) -> Self {
        Self::build(self.len + other.len, |i| {
            if i < self.len {
                self.at(i)
            } else {
                other.at(i - self.len)
            }
        })
    }
    pub fn from_nibbles_unsafe(nibbles: Vec<u8>) -> Self {
        Self::build(nibbles.len(), |i| nibbles[i])
    }
}
