use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Nibbles {
    // Stored as one nibble per byte (0x00..0x0F) for easier manipulation
    data: Vec<u8>,
}

impl Nibbles {
    // pub fn new() -> Self {
    //     Self { data: Vec::new() }
    // }

    pub fn from_raw(key: &[u8], is_leaf: bool) -> Self {
        let mut data = Vec::with_capacity(key.len() * 2);
        for &b in key {
            data.push(b >> 4);
            data.push(b & 0x0F);
        }
        if is_leaf {
            // In some MPT implementations, there's a terminator.
            // But usually we handle leaf vs extension by node type.
            // We'll keep it simple: raw nibbles.
        }
        Self { data }
    }

    pub fn from_nibbles_unsafe(nibbles: Vec<u8>) -> Self {
        Self { data: nibbles }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn at(&self, index: usize) -> u8 {
        self.data[index]
    }

    // pub fn slice(&self, start: usize, end: usize) -> Self {
    //     Self {
    //         data: self.data[start..end].to_vec(),
    //     }
    // }

    pub fn common_prefix(&self, other: &Nibbles) -> usize {
        let len = std::cmp::min(self.len(), other.len());
        let mut i = 0;
        while i < len && self.data[i] == other.data[i] {
            i += 1;
        }
        i
    }

    pub fn starts_with(&self, other: &Nibbles) -> bool {
        if self.len() < other.len() {
            return false;
        }
        &self.data[..other.len()] == other.data.as_slice()
    }

    pub fn split_at(&self, idx: usize) -> (Self, Self) {
        let (a, b) = self.data.split_at(idx);
        (Self { data: a.to_vec() }, Self { data: b.to_vec() })
    }

    // pub fn join(&self, other: &Nibbles) -> Self {
    //     let mut data = self.data.clone();
    //     data.extend_from_slice(&other.data);
    //     Self { data }
    // }

    // pub fn push(&mut self, nibble: u8) {
    //     debug_assert!(nibble < 16);
    //     self.data.push(nibble);
    // }

    // pub fn pop(&mut self) -> Option<u8> {
    //     self.data.pop()
    // }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Debug for Nibbles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Nibbles(")?;
        for n in &self.data {
            write!(f, "{:x}", n)?;
        }
        write!(f, ")")
    }
}
