/// Segment file abstractions for the append-only value log.
///
/// `MmapSegment` reads sealed (immutable) segments via memory-mapping.
/// `SegmentWriter` handles buffered append-only writes to the active segment.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use super::record;

/// Default maximum segment size: 256 MB.
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;

/// Flush the BufWriter once the internal encode buffer exceeds 64 KB.
pub const FLUSH_THRESHOLD: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Build the canonical path for a segment file.
pub fn segment_path(dir: &Path, segment_id: u32) -> PathBuf {
    dir.join(format!("segment-{:06}.vlog", segment_id))
}

/// Build the canonical path for a hint file that accompanies a segment.
pub fn hint_path(dir: &Path, segment_id: u32) -> PathBuf {
    dir.join(format!("hint-{:06}.hint", segment_id))
}

// ---------------------------------------------------------------------------
// MmapSegment — read-only access to a sealed segment
// ---------------------------------------------------------------------------

/// A memory-mapped, read-only view of a sealed segment file.
pub struct MmapSegment {
    id: u32,
    mmap: Mmap,
    path: PathBuf,
}

impl MmapSegment {
    /// Open an existing segment file and memory-map it.
    pub fn open(path: &Path, id: u32) -> io::Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the file is sealed (immutable) so the mapping stays valid.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            id,
            mmap,
            path: path.to_path_buf(),
        })
    }

    /// Return a bounds-checked slice starting at `offset` with length `len`.
    pub fn read_at(&self, offset: u64, len: u32) -> Option<&[u8]> {
        let start = offset as usize;
        let end = start.checked_add(len as usize)?;
        if end > self.mmap.len() {
            return None;
        }
        Some(&self.mmap[start..end])
    }

    /// Full mmap contents (useful for sequential iteration during recovery).
    pub fn data(&self) -> &[u8] {
        &self.mmap
    }

    /// Length of the mapped region in bytes.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// File path of this segment.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ---------------------------------------------------------------------------
// SegmentWriter — buffered append-only writer for the active segment
// ---------------------------------------------------------------------------

/// Buffered, append-only writer for the currently active segment file.
pub struct SegmentWriter {
    /// Scratch buffer reused for encoding each record before writing.
    buffer: Vec<u8>,
    /// Buffered file handle.
    file: BufWriter<File>,
    /// Numeric id of this segment.
    segment_id: u32,
    /// Byte offset where the next write will land.
    current_offset: u64,
    /// Segment is considered full when `current_offset >= max_size`.
    max_size: u64,
    /// Directory that contains this segment file.
    dir: PathBuf,
}

impl SegmentWriter {
    /// Create (or re-open) the active segment file inside `dir`.
    ///
    /// The directory is created if it does not exist. If the file already has
    /// data the writer resumes from the end.
    pub fn new(dir: &Path, segment_id: u32, max_size: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let path = segment_path(dir, segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let current_offset = file.metadata()?.len();

        Ok(Self {
            buffer: Vec::with_capacity(4096),
            file: BufWriter::new(file),
            segment_id,
            current_offset,
            max_size,
            dir: dir.to_path_buf(),
        })
    }

    /// Append a PUT record and return the byte offset where it was written.
    pub fn append_put(
        &mut self,
        lsn: u64,
        prev_offset: u64,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<u64> {
        self.buffer.clear();
        record::encode_put(&mut self.buffer, lsn, prev_offset, key, value);
        self.write_buffer()
    }

    /// Append a TOMBSTONE record and return the byte offset where it was written.
    pub fn append_tombstone(
        &mut self,
        lsn: u64,
        prev_offset: u64,
        key: &[u8],
    ) -> io::Result<u64> {
        self.buffer.clear();
        record::encode_tombstone(&mut self.buffer, lsn, prev_offset, key);
        self.write_buffer()
    }

    /// Flush the BufWriter and call fdatasync to ensure durability.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }

    /// Returns `true` when the segment has reached (or exceeded) its size cap.
    pub fn should_seal(&self) -> bool {
        self.current_offset >= self.max_size
    }

    /// Flush all pending data and return the path of the sealed segment.
    pub fn seal(mut self) -> io::Result<PathBuf> {
        self.flush()?;
        Ok(segment_path(&self.dir, self.segment_id))
    }

    // -- internal -----------------------------------------------------------

    /// Write the contents of `self.buffer` to the file, updating the offset
    /// and flushing when the BufWriter's internal buffer is large enough.
    fn write_buffer(&mut self) -> io::Result<u64> {
        let record_offset = self.current_offset;
        self.file.write_all(&self.buffer)?;
        self.current_offset += self.buffer.len() as u64;

        if self.file.buffer().len() >= FLUSH_THRESHOLD {
            self.flush()?;
        }

        Ok(record_offset)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{decode_record, FLAG_PUT, FLAG_TOMBSTONE, NO_PREV_OFFSET};

    #[test]
    fn test_segment_writer_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), 0, DEFAULT_MAX_SEGMENT_SIZE).unwrap();

        let off0 = writer
            .append_put(1, NO_PREV_OFFSET, b"key1", b"value1")
            .unwrap();
        let off1 = writer
            .append_put(2, off0, b"key2", b"value2")
            .unwrap();
        writer.flush().unwrap();

        // Memory-map the written segment and decode both records.
        let seg = MmapSegment::open(&segment_path(dir.path(), 0), 0).unwrap();
        let data = seg.data();

        let (hdr0, next) = decode_record(data, off0 as usize).expect("decode record 0");
        assert_eq!(hdr0.lsn, 1);
        assert_eq!(hdr0.prev_offset, NO_PREV_OFFSET);
        assert_eq!(hdr0.key, b"key1");
        assert_eq!(hdr0.value_len, 6);
        assert_eq!(hdr0.flags, FLAG_PUT);
        assert_eq!(next, off1 as usize);

        let (hdr1, _) = decode_record(data, off1 as usize).expect("decode record 1");
        assert_eq!(hdr1.lsn, 2);
        assert_eq!(hdr1.prev_offset, off0);
        assert_eq!(hdr1.key, b"key2");
        assert_eq!(hdr1.value_len, 6);
        assert_eq!(hdr1.flags, FLAG_PUT);
    }

    #[test]
    fn test_segment_writer_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), 0, DEFAULT_MAX_SEGMENT_SIZE).unwrap();

        let off_put = writer
            .append_put(1, NO_PREV_OFFSET, b"delme", b"val")
            .unwrap();
        let off_tomb = writer
            .append_tombstone(2, off_put, b"delme")
            .unwrap();
        writer.flush().unwrap();

        let seg = MmapSegment::open(&segment_path(dir.path(), 0), 0).unwrap();
        let data = seg.data();

        let (hdr_put, _) = decode_record(data, off_put as usize).expect("decode PUT");
        assert_eq!(hdr_put.flags, FLAG_PUT);

        let (hdr_tomb, _) = decode_record(data, off_tomb as usize).expect("decode TOMBSTONE");
        assert_eq!(hdr_tomb.flags, FLAG_TOMBSTONE);
        assert_eq!(hdr_tomb.value_len, 0);
        assert_eq!(hdr_tomb.prev_offset, off_put);
    }

    #[test]
    fn test_mmap_segment_read_at() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), 1, DEFAULT_MAX_SEGMENT_SIZE).unwrap();

        let key = b"rkey";
        let value = b"rvalue";
        let off = writer
            .append_put(10, NO_PREV_OFFSET, key, value)
            .unwrap();
        writer.flush().unwrap();

        let seg = MmapSegment::open(&segment_path(dir.path(), 1), 1).unwrap();

        // Value starts at: offset + 8(lsn) + 8(prev) + 4(key_len) + key.len() + 4(val_len)
        let val_start = off + 8 + 8 + 4 + key.len() as u64 + 4;
        let slice = seg
            .read_at(val_start, value.len() as u32)
            .expect("read_at should succeed");
        assert_eq!(slice, value);

        // Out-of-bounds read must return None.
        assert!(seg.read_at(val_start, (seg.len() + 1) as u32).is_none());
    }

    #[test]
    fn test_should_seal() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), 0, 100).unwrap();

        assert!(!writer.should_seal());

        // Write enough data to exceed the 100-byte cap.
        // Each record has 29 bytes of fixed overhead plus key + value lengths.
        writer
            .append_put(1, NO_PREV_OFFSET, b"bigkey", &[0u8; 80])
            .unwrap();

        assert!(
            writer.should_seal(),
            "should_seal must be true after writing >100 bytes"
        );
    }
}
