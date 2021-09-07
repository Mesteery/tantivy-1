use crate::directory::FileSlice;
use crate::directory::OwnedBytes;
use crate::space_usage::ByteCount;
use crate::DocId;
use common::BitSet;
use common::HasLen;
use std::io;
use std::io::Write;

/// Write a delete `BitSet`
///
/// where `delete_bitset` is the set of deleted `DocId`.
/// Warning: this function does not call terminate. The caller is in charge of
/// closing the writer properly.
pub fn write_delete_bitset(
    delete_bitset: &BitSet,
    max_doc: u32,
    writer: &mut dyn Write,
) -> io::Result<()> {
    let mut byte = 0u8;
    let mut shift = 0u8;
    for doc in 0..max_doc {
        if delete_bitset.contains(doc) {
            byte |= 1 << shift;
        }
        if shift == 7 {
            writer.write_all(&[byte])?;
            shift = 0;
            byte = 0;
        } else {
            shift += 1;
        }
    }
    if max_doc % 8 > 0 {
        writer.write_all(&[byte])?;
    }
    Ok(())
}

/// Merges two `DeleteBitSet` into a new one.
pub fn merge_delete_bitset(left: &DeleteBitSet, right: &DeleteBitSet) -> DeleteBitSet {
    let left_data = left.data.as_slice();
    let right_data = right.data.as_slice();

    let mut merged = vec![];
    merged.resize(left_data.len().max(right_data.len()), 0);

    for (merged_el, left_el) in merged.iter_mut().zip(left_data.iter()) {
        *merged_el = *left_el;
    }

    for (merged_el, right_el) in merged.iter_mut().zip(right_data.iter()) {
        *merged_el |= *right_el;
    }

    let num_deleted: usize = merged
        .as_slice()
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum();

    DeleteBitSet {
        data: OwnedBytes::new(merged),
        num_deleted,
    }
}

/// Set of deleted `DocId`s.
#[derive(Clone)]
pub struct DeleteBitSet {
    data: OwnedBytes,
    num_deleted: usize,
}

impl DeleteBitSet {
    pub(crate) fn from_bitset(bitset: &BitSet, max_doc: u32) -> DeleteBitSet {
        let mut out = vec![];
        write_delete_bitset(&bitset, max_doc, &mut out).unwrap();

        DeleteBitSet {
            data: OwnedBytes::new(out),
            num_deleted: bitset.len(),
        }
    }
    #[cfg(test)]
    pub(crate) fn for_test(docs: &[DocId], max_doc: u32) -> DeleteBitSet {
        use crate::directory::{Directory, RamDirectory, TerminatingWrite};
        use std::path::Path;
        assert!(docs.iter().all(|&doc| doc < max_doc));
        let mut bitset = BitSet::with_max_value(max_doc);
        for &doc in docs {
            bitset.insert(doc);
        }
        let directory = RamDirectory::create();
        let path = Path::new("dummydeletebitset");
        let mut wrt = directory.open_write(path).unwrap();
        write_delete_bitset(&bitset, max_doc, &mut wrt).unwrap();
        wrt.terminate().unwrap();
        let file = directory.open_read(path).unwrap();
        Self::open(file).unwrap()
    }

    /// Opens a delete bitset given its file.
    pub fn open(file: FileSlice) -> crate::Result<DeleteBitSet> {
        let bytes = file.read_bytes()?;
        let num_deleted: usize = bytes
            .as_slice()
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        Ok(DeleteBitSet {
            data: bytes,
            num_deleted,
        })
    }

    /// Returns true iff the document is still "alive". In other words, if it has not been deleted.
    pub fn is_alive(&self, doc: DocId) -> bool {
        !self.is_deleted(doc)
    }

    /// Returns true iff the document has been marked as deleted.
    #[inline]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        let byte_offset = doc / 8u32;
        let b: u8 = self.data.as_slice()[byte_offset as usize];
        let shift = (doc & 7u32) as u8;
        b & (1u8 << shift) != 0
    }

    /// The number of deleted docs
    pub fn num_deleted(&self) -> usize {
        self.num_deleted
    }
    /// Summarize total space usage of this bitset.
    pub fn space_usage(&self) -> ByteCount {
        self.data.len()
    }
}

impl HasLen for DeleteBitSet {
    fn len(&self) -> usize {
        self.num_deleted
    }
}

#[cfg(test)]
mod tests {
    use crate::fastfield::delete::merge_delete_bitset;

    use super::DeleteBitSet;
    use common::HasLen;

    #[test]
    fn test_delete_bitset_empty() {
        let delete_bitset = DeleteBitSet::for_test(&[], 10);
        for doc in 0..10 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
        assert_eq!(delete_bitset.len(), 0);
    }

    #[test]
    fn test_delete_bitset() {
        let delete_bitset = DeleteBitSet::for_test(&[1, 9], 10);
        assert!(delete_bitset.is_alive(0));
        assert!(delete_bitset.is_deleted(1));
        assert!(delete_bitset.is_alive(2));
        assert!(delete_bitset.is_alive(3));
        assert!(delete_bitset.is_alive(4));
        assert!(delete_bitset.is_alive(5));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(7));
        assert!(delete_bitset.is_alive(8));
        assert!(delete_bitset.is_deleted(9));
        for doc in 0..10 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
        assert_eq!(delete_bitset.len(), 2);
    }

    #[test]
    fn test_delete_bitset_merge() {
        let delete_bitset1 = DeleteBitSet::for_test(&[1, 9], 10);
        let delete_bitset2 = DeleteBitSet::for_test(&[1, 5, 9, 14], 15);
        let delete_bitset = merge_delete_bitset(&delete_bitset1, &delete_bitset2);
        assert!(delete_bitset.is_alive(0));
        assert!(delete_bitset.is_deleted(1));
        assert!(delete_bitset.is_alive(2));
        assert!(delete_bitset.is_alive(3));
        assert!(delete_bitset.is_alive(4));
        assert!(delete_bitset.is_deleted(5));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(7));
        assert!(delete_bitset.is_alive(8));
        assert!(delete_bitset.is_deleted(9));
        assert!(delete_bitset.is_alive(10));
        assert!(delete_bitset.is_alive(11));
        assert!(delete_bitset.is_alive(12));
        assert!(delete_bitset.is_alive(13));
        assert!(delete_bitset.is_deleted(14));
        assert!(delete_bitset.is_alive(15));
        for doc in 0..15 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
    }
}
