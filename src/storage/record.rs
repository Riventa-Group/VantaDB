/// Segment record format with CRC32C encoding/decoding.
///
/// On-disk record layout:
/// ```text
/// [lsn:8][prev_offset:8][key_len:4][key:var][val_len:4][value:var][flags:1][crc32c:4]
/// ```

/// Flag indicating a PUT (insert/update) record.
pub const FLAG_PUT: u8 = 0x01;

/// Flag indicating a tombstone (delete) record.
pub const FLAG_TOMBSTONE: u8 = 0x02;

/// Sentinel value meaning "no previous offset exists."
pub const NO_PREV_OFFSET: u64 = u64::MAX;

/// Fixed-size pointer into a segment file. Must be exactly 24 bytes.
///
/// Field order chosen so the two u64 fields come first, avoiding padding
/// under `#[repr(C)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct EntryPointer {
    pub lsn: u64,
    pub offset: u64,
    pub segment_id: u32,
    pub value_len: u32,
}

/// Decoded header of a single record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub lsn: u64,
    pub prev_offset: u64,
    pub key: Vec<u8>,
    pub value_len: u32,
    pub flags: u8,
    pub total_len: usize,
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Append a PUT record to `buf` and return the number of bytes written.
pub fn encode_put(buf: &mut Vec<u8>, lsn: u64, prev_offset: u64, key: &[u8], value: &[u8]) -> usize {
    encode_record(buf, lsn, prev_offset, key, value, FLAG_PUT)
}

/// Append a TOMBSTONE (delete) record to `buf` and return the number of bytes written.
pub fn encode_tombstone(buf: &mut Vec<u8>, lsn: u64, prev_offset: u64, key: &[u8]) -> usize {
    encode_record(buf, lsn, prev_offset, key, &[], FLAG_TOMBSTONE)
}

fn encode_record(
    buf: &mut Vec<u8>,
    lsn: u64,
    prev_offset: u64,
    key: &[u8],
    value: &[u8],
    flags: u8,
) -> usize {
    let start = buf.len();

    buf.extend_from_slice(&lsn.to_le_bytes());            // 8
    buf.extend_from_slice(&prev_offset.to_le_bytes());     // 8
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes()); // 4
    buf.extend_from_slice(key);                            // var
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes()); // 4
    buf.extend_from_slice(value);                          // var
    buf.push(flags);                                       // 1

    // CRC32C covers everything from `start` up to (but not including) the checksum.
    let crc = crc32c::crc32c(&buf[start..]);
    buf.extend_from_slice(&crc.to_le_bytes());             // 4

    buf.len() - start
}

// ---------------------------------------------------------------------------
// Decoding helpers
// ---------------------------------------------------------------------------

/// Minimum fixed-overhead bytes in a record (excluding variable-length key/value):
/// lsn(8) + prev_offset(8) + key_len(4) + val_len(4) + flags(1) + crc(4) = 29
const MIN_RECORD_LEN: usize = 8 + 8 + 4 + 4 + 1 + 4;

/// Decode a record starting at `pos` in `data`.
///
/// Returns `None` when:
/// - the data is truncated (not enough bytes), or
/// - the CRC32C checksum does not match.
pub fn decode_record(data: &[u8], pos: usize) -> Option<(RecordHeader, usize)> {
    let remaining = data.len().checked_sub(pos)?;
    if remaining < MIN_RECORD_LEN {
        return None;
    }

    let mut cursor = pos;

    let lsn = u64::from_le_bytes(data[cursor..cursor + 8].try_into().ok()?);
    cursor += 8;

    let prev_offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().ok()?);
    cursor += 8;

    let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;

    if data.len() < cursor + key_len + 4 + 1 + 4 {
        return None; // truncated before key + val_len + flags + crc
    }

    let key = data[cursor..cursor + key_len].to_vec();
    cursor += key_len;

    let value_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?);
    cursor += 4;

    let val_len = value_len as usize;
    if data.len() < cursor + val_len + 1 + 4 {
        return None; // truncated before value + flags + crc
    }

    cursor += val_len; // skip over value bytes

    let flags = data[cursor];
    cursor += 1;

    // CRC covers [pos .. cursor)
    let stored_crc = u32::from_le_bytes(data[cursor..cursor + 4].try_into().ok()?);
    let computed_crc = crc32c::crc32c(&data[pos..cursor]);
    if stored_crc != computed_crc {
        return None;
    }
    cursor += 4;

    let total_len = cursor - pos;

    let header = RecordHeader {
        lsn,
        prev_offset,
        key,
        value_len,
        flags,
        total_len,
    };

    Some((header, cursor))
}

/// Extract the raw value bytes from a record at a known position.
///
/// This avoids a full decode when only the value payload is needed.
pub fn extract_value(data: &[u8], record_offset: usize, key_len: u32, value_len: u32) -> Option<&[u8]> {
    // Value starts after: lsn(8) + prev_offset(8) + key_len_field(4) + key(key_len) + val_len_field(4)
    let value_start = record_offset
        .checked_add(8 + 8 + 4)?
        .checked_add(key_len as usize)?
        .checked_add(4)?;
    let value_end = value_start.checked_add(value_len as usize)?;
    if value_end > data.len() {
        return None;
    }
    Some(&data[value_start..value_end])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_put() {
        let mut buf = Vec::new();
        let lsn = 42u64;
        let prev = NO_PREV_OFFSET;
        let key = b"hello";
        let value = b"world";

        let written = encode_put(&mut buf, lsn, prev, key, value);
        assert_eq!(buf.len(), written);

        let (hdr, next_pos) = decode_record(&buf, 0).expect("decode should succeed");
        assert_eq!(hdr.lsn, lsn);
        assert_eq!(hdr.prev_offset, prev);
        assert_eq!(hdr.key, key.to_vec());
        assert_eq!(hdr.value_len, value.len() as u32);
        assert_eq!(hdr.flags, FLAG_PUT);
        assert_eq!(hdr.total_len, written);
        assert_eq!(next_pos, written);
    }

    #[test]
    fn test_encode_decode_tombstone() {
        let mut buf = Vec::new();
        let lsn = 99u64;
        let prev = 1024u64;
        let key = b"deleted_key";

        let written = encode_tombstone(&mut buf, lsn, prev, key);
        assert_eq!(buf.len(), written);

        let (hdr, next_pos) = decode_record(&buf, 0).expect("decode should succeed");
        assert_eq!(hdr.lsn, lsn);
        assert_eq!(hdr.prev_offset, prev);
        assert_eq!(hdr.key, key.to_vec());
        assert_eq!(hdr.value_len, 0);
        assert_eq!(hdr.flags, FLAG_TOMBSTONE);
        assert_eq!(next_pos, written);
    }

    #[test]
    fn test_crc_corruption_detected() {
        let mut buf = Vec::new();
        encode_put(&mut buf, 1, NO_PREV_OFFSET, b"k", b"v");

        // Corrupt a byte in the middle of the record.
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;

        assert!(
            decode_record(&buf, 0).is_none(),
            "corrupted record should fail CRC check"
        );
    }

    #[test]
    fn test_multiple_records_sequential() {
        let mut buf = Vec::new();

        let keys: [&[u8]; 3] = [b"key1", b"key2", b"key3"];
        let vals: [&[u8]; 3] = [b"val1", b"val2", b"val3"];

        for i in 0..3 {
            encode_put(&mut buf, i as u64, NO_PREV_OFFSET, keys[i], vals[i]);
        }

        let mut pos = 0usize;
        for i in 0..3 {
            let (hdr, next) = decode_record(&buf, pos).expect("decode should succeed");
            assert_eq!(hdr.lsn, i as u64);
            assert_eq!(hdr.key, keys[i].to_vec());
            assert_eq!(hdr.value_len, vals[i].len() as u32);
            assert_eq!(hdr.flags, FLAG_PUT);
            pos = next;
        }

        // After the last record, pos should equal the total buffer length.
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn test_extract_value() {
        let mut buf = Vec::new();
        let key = b"extract_me";
        let value = b"payload_data";

        encode_put(&mut buf, 7, NO_PREV_OFFSET, key, value);

        let extracted = extract_value(&buf, 0, key.len() as u32, value.len() as u32)
            .expect("extract should succeed");
        assert_eq!(extracted, value);
    }

    #[test]
    fn test_entry_pointer_size() {
        assert_eq!(std::mem::size_of::<EntryPointer>(), 24);
    }
}
