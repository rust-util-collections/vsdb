//! Length and bounds checks for MPT proof decoding.
use super::{Result, invalid_state};
pub(crate) fn read_varint(data: &[u8], cursor: &mut usize) -> Result<usize> {
    let mut n: usize = 0;
    let mut shift: u32 = 0;
    loop {
        if *cursor >= data.len() {
            return Err(invalid_state("varint unexpected EOF".into()));
        }
        let b = data[*cursor];
        *cursor += 1;
        if shift >= usize::BITS {
            return Err(invalid_state("varint overflow".into()));
        }
        let low = (b & 0x7F) as usize;
        if low > (usize::MAX >> shift) {
            return Err(invalid_state("varint overflow".into()));
        }
        let val = low << shift;
        n = n
            .checked_add(val)
            .ok_or_else(|| invalid_state("varint overflow".into()))?;
        if b & 0x80 == 0 {
            if shift > 0 && low == 0 {
                return Err(invalid_state("non-canonical varint encoding".into()));
            }
            break;
        }
        shift += 7;
    }
    Ok(n)
}

pub(crate) fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_varint(data, cursor)?;
    let end = checked_end(*cursor, len, data.len(), "bytes")?;
    let bytes = data[*cursor..end].to_vec();
    *cursor = end;
    Ok(bytes)
}

pub(crate) fn checked_end(
    cursor: usize,
    len: usize,
    total: usize,
    what: &str,
) -> Result<usize> {
    cursor
        .checked_add(len)
        .filter(|end| *end <= total)
        .ok_or_else(|| invalid_state(format!("{what} unexpected EOF")))
}

pub(crate) fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor >= data.len() {
        return Err(invalid_state("unexpected EOF".into()));
    }
    let v = data[*cursor];
    *cursor += 1;
    Ok(v)
}
