//! Binary codec + CRC32 framing used by the file adapters. Hand-written to
//! keep the crate dependency-free and the on-disk format explicit:
//!
//! ```text
//! record := len:u32le crc32(payload):u32le payload[len]
//! ```

use crate::domain::types::*;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub &'static str);

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "codec: {}", self.0)
    }
}
impl std::error::Error for CodecError {}

/// Upper bound for one record, to avoid allocating on garbage lengths.
pub const MAX_RECORD: usize = 64 * 1024 * 1024;

const fn crc_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}
static CRC_TABLE: [u32; 256] = crc_table();

/// CRC-32 (IEEE 802.3).
pub fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = CRC_TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Parses consecutive frames. Returns the payloads and the byte offset of
/// the end of the last valid frame (a torn/corrupt tail stops parsing).
pub fn unframe_all(buf: &[u8]) -> (Vec<&[u8]>, usize) {
    let mut out = Vec::new();
    let mut pos = 0;
    while buf.len() - pos >= 8 {
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().expect("4 bytes"));
        if len > MAX_RECORD || buf.len() - pos - 8 < len {
            break;
        }
        let payload = &buf[pos + 8..pos + 8 + len];
        if crc32(payload) != crc {
            break;
        }
        out.push(payload);
        pos += 8 + len;
    }
    (out, pos)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.buf.len() - self.pos < n {
            return Err(CodecError("truncated"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, CodecError> {
        let n = self.u32()? as usize;
        if n > MAX_RECORD {
            return Err(CodecError("length too large"));
        }
        Ok(self.take(n)?.to_vec())
    }
    fn finish(&self) -> Result<(), CodecError> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(CodecError("trailing bytes"))
        }
    }
}

fn put_config(out: &mut Vec<u8>, c: &Configuration) {
    out.extend_from_slice(&(c.voters.len() as u32).to_le_bytes());
    for v in &c.voters {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn get_config(r: &mut Reader<'_>) -> Result<Configuration, CodecError> {
    let n = r.u32()? as usize;
    if n > 1 << 16 {
        return Err(CodecError("too many voters"));
    }
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(r.u64()?);
    }
    Ok(Configuration::new(ids))
}

pub fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&e.term.to_le_bytes());
    out.extend_from_slice(&e.index.to_le_bytes());
    match &e.kind {
        EntryKind::Noop => out.push(0),
        EntryKind::Command(d) => {
            out.push(1);
            out.extend_from_slice(&(d.len() as u32).to_le_bytes());
            out.extend_from_slice(d);
        }
        EntryKind::Config(c) => {
            out.push(2);
            put_config(&mut out, c);
        }
    }
    out
}

pub fn decode_entry(buf: &[u8]) -> Result<Entry, CodecError> {
    let mut r = Reader { buf, pos: 0 };
    let term = r.u64()?;
    let index = r.u64()?;
    let kind = match r.u8()? {
        0 => EntryKind::Noop,
        1 => EntryKind::Command(r.bytes()?),
        2 => EntryKind::Config(get_config(&mut r)?),
        _ => return Err(CodecError("unknown entry kind")),
    };
    r.finish()?;
    Ok(Entry { term, index, kind })
}

pub fn encode_hard_state(hs: &HardState) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&hs.term.to_le_bytes());
    out.extend_from_slice(&hs.voted_for.unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&hs.commit.to_le_bytes());
    out
}

pub fn decode_hard_state(buf: &[u8]) -> Result<HardState, CodecError> {
    let mut r = Reader { buf, pos: 0 };
    let term = r.u64()?;
    let v = r.u64()?;
    let commit = r.u64()?;
    r.finish()?;
    Ok(HardState {
        term,
        voted_for: (v != 0).then_some(v),
        commit,
    })
}

pub fn encode_snapshot(s: &Snapshot) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&s.meta.index.to_le_bytes());
    out.extend_from_slice(&s.meta.term.to_le_bytes());
    put_config(&mut out, &s.meta.config);
    out.extend_from_slice(&(s.data.len() as u32).to_le_bytes());
    out.extend_from_slice(&s.data);
    out
}

pub fn decode_snapshot(buf: &[u8]) -> Result<Snapshot, CodecError> {
    let mut r = Reader { buf, pos: 0 };
    let index = r.u64()?;
    let term = r.u64()?;
    let config = get_config(&mut r)?;
    let data = r.bytes()?;
    r.finish()?;
    Ok(Snapshot {
        meta: SnapshotMeta {
            index,
            term,
            config,
        },
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn entry_roundtrip_all_kinds() {
        for kind in [
            EntryKind::Noop,
            EntryKind::Command(b"set x=1".to_vec()),
            EntryKind::Config(Configuration::new([1, 2, 3])),
        ] {
            let e = Entry {
                term: 7,
                index: 42,
                kind,
            };
            assert_eq!(decode_entry(&encode_entry(&e)).unwrap(), e);
        }
    }

    #[test]
    fn torn_tail_is_ignored() {
        let mut buf = frame(b"one");
        buf.extend(frame(b"two"));
        let good = buf.len();
        buf.extend(&frame(b"three")[..5]);
        let (recs, end) = unframe_all(&buf);
        assert_eq!(recs, vec![&b"one"[..], &b"two"[..]]);
        assert_eq!(end, good);
    }

    #[test]
    fn corrupt_crc_stops_parsing() {
        let mut buf = frame(b"one");
        let mut bad = frame(b"two");
        let last = bad.len() - 1;
        bad[last] ^= 0xFF;
        buf.extend(bad);
        let (recs, _) = unframe_all(&buf);
        assert_eq!(recs.len(), 1);
    }

    #[test]
    fn garbage_never_panics() {
        for seed in 0u8..=255 {
            let buf: Vec<u8> = (0..64)
                .map(|i| seed.wrapping_mul(31).wrapping_add(i))
                .collect();
            let _ = decode_entry(&buf);
            let _ = decode_snapshot(&buf);
            let _ = unframe_all(&buf);
        }
    }
}
