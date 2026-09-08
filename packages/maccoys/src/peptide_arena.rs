use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error returned by [`PeptideArena::try_push`] when a sequence is too long to store.
///
#[derive(Debug, Error)]
#[error("sequence is {0} bytes long, exceeding the maximum supported length of 65535")]
pub struct SequenceTooLongError(pub usize);

/// Peptide buffer (`arena`) holding
/// every sequence's bytes back-to-back, plus a per-sequence byte length (`lengths`),
/// instead of a `Vec<String>` with one heap allocation per sequence. At up to ~1M
/// sequences this avoids ~1M small allocations.
///
/// A sequence is assumed to never exceed `u16::MAX` (65535) bytes
/// are bounded to 50 amino acids (max peptide length in MaCPepDB), and even heavy PTM/ProForma annotation overhead
/// stays far below that) — [`PeptideArena::push`] panics and
/// [`PeptideArena::try_push`] errors if that's violated.
///
/// Since only per-sequence lengths are stored, not cumulative offsets, [`PeptideArena::get`]
/// is O(idx); prefer [`PeptideArena::iter`] (O(n) total) over calling it in a loop.
///
#[derive(Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PeptideArena {
    arena: String,
    lengths: Vec<u16>,
}

impl PeptideArena {
    /// Creates a new, empty arena.
    ///
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new, empty arena with preallocated capacity for `capacity` sequences.
    ///
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            // Heuristic average peptide length, just to size the initial allocation.
            arena: String::with_capacity(capacity * 12),
            lengths: Vec::with_capacity(capacity),
        }
    }

    /// Appends `sequence` to the arena.
    ///
    /// # Panics
    /// If `sequence` is longer than `u16::MAX` (65535) bytes.
    ///
    pub fn push(&mut self, sequence: impl AsRef<str>) {
        self.try_push(sequence)
            .unwrap_or_else(|err| panic!("peptide {err}"));
    }

    /// Appends `sequence` to the arena, or returns [`SequenceTooLongError`] if it's
    /// longer than `u16::MAX` (65535) bytes.
    ///
    pub fn try_push(&mut self, sequence: impl AsRef<str>) -> Result<(), SequenceTooLongError> {
        let sequence = sequence.as_ref();
        let len =
            u16::try_from(sequence.len()).map_err(|_| SequenceTooLongError(sequence.len()))?;
        self.lengths.push(len);
        self.arena.push_str(sequence);
        Ok(())
    }

    /// Returns the number of sequences in the arena.
    ///
    pub fn len(&self) -> usize {
        self.lengths.len()
    }

    /// Returns `true` if the arena contains no sequences.
    ///
    pub fn is_empty(&self) -> bool {
        self.lengths.is_empty()
    }

    /// Returns the `idx`-th sequence. O(idx) — prefer [`Self::iter`] in a loop.
    ///
    pub fn get(&self, idx: usize) -> &str {
        let start: usize = self.lengths[..idx].iter().map(|&len| len as usize).sum();
        let end = start + self.lengths[idx] as usize;
        &self.arena[start..end]
    }

    /// Returns an iterator over every sequence in the arena, in insertion order. O(n)
    /// total, unlike calling [`Self::get`] in a loop.
    ///
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        let mut pos = 0usize;
        self.lengths.iter().map(move |&len| {
            let start = pos;
            pos += len as usize;
            &self.arena[start..pos]
        })
    }

    /// Heap bytes used by the arena's buffer and length index, for size accounting.
    ///
    pub fn size(&self) -> usize {
        self.arena.capacity() + self.lengths.capacity() * std::mem::size_of::<u16>()
    }

    /// Appends every sequence from `other` to this arena, in order, leaving `other`
    /// empty. Cheaper than re-`push`ing each of `other`'s sequences one by one.
    ///
    pub fn merge(&mut self, other: Self) {
        self.arena.reserve(other.arena.len());
        self.arena.push_str(&other.arena);
        self.lengths.extend(other.lengths);
    }

    /// Removes every sequence at an index where `keep[idx]` is `false`, keeping the
    /// relative order of the retained sequences. Rebuilds the arena's buffer (since
    /// sequences are stored back-to-back rather than as separate allocations), so this
    /// is O(n) in the arena's total byte length, not O(removed sequences).
    ///
    /// # Panics
    /// If `keep.len() != self.len()`.
    ///
    pub fn retain(&mut self, keep: &[bool]) {
        assert_eq!(
            keep.len(),
            self.len(),
            "keep mask length ({}) must match arena length ({})",
            keep.len(),
            self.len()
        );

        let mut new_arena = String::with_capacity(self.arena.len());
        let mut new_lengths = Vec::with_capacity(self.lengths.len());
        let mut pos = 0usize;
        for (&len, &keep) in self.lengths.iter().zip(keep) {
            let seq = &self.arena[pos..pos + len as usize];
            pos += len as usize;
            if keep {
                new_arena.push_str(seq);
                new_lengths.push(len);
            }
        }
        self.arena = new_arena;
        self.lengths = new_lengths;
    }
}

impl From<PeptideArena> for Vec<String> {
    fn from(value: PeptideArena) -> Self {
        let mut sequences = Vec::with_capacity(value.lengths.len());
        let mut pos = 0usize;
        for len in value.lengths {
            let len = len as usize;
            sequences.push(value.arena[pos..pos + len].to_string());
            pos += len;
        }
        sequences
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_get_iter() {
        let mut arena = PeptideArena::new();
        arena.push("PEPTIDE");
        #[allow(clippy::unnecessary_to_owned)]
        arena.push("PEPTIDEK".to_string());
        arena.push("SEQVENCE");

        assert_eq!(arena.len(), 3);
        assert!(!arena.is_empty());
        assert_eq!(arena.get(0), "PEPTIDE");
        assert_eq!(arena.get(1), "PEPTIDEK");
        assert_eq!(arena.get(2), "SEQVENCE");
        assert_eq!(
            arena.iter().collect::<Vec<_>>(),
            vec!["PEPTIDE", "PEPTIDEK", "SEQVENCE"]
        );
    }

    #[test]
    fn test_into_vec_string() {
        let mut arena = PeptideArena::new();
        arena.push("PEPTIDE");
        arena.push("PEPTIDEK");

        let sequences: Vec<String> = arena.into();
        assert_eq!(
            sequences,
            vec!["PEPTIDE".to_string(), "PEPTIDEK".to_string()]
        );
    }

    #[test]
    fn test_merge() {
        let mut a = PeptideArena::new();
        a.push("PEPTIDE");
        a.push("PEPTIDEK");

        let mut b = PeptideArena::new();
        b.push("SEQVENCE");

        a.merge(b);

        assert_eq!(a.len(), 3);
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec!["PEPTIDE", "PEPTIDEK", "SEQVENCE"]
        );
    }

    #[test]
    fn test_retain() {
        let mut arena = PeptideArena::new();
        arena.push("PEPTIDE");
        arena.push("PEPTIDEK");
        arena.push("SEQVENCE");

        arena.retain(&[true, false, true]);

        assert_eq!(arena.len(), 2);
        assert_eq!(
            arena.iter().collect::<Vec<_>>(),
            vec!["PEPTIDE", "SEQVENCE"]
        );
    }

    #[test]
    #[should_panic]
    fn test_retain_mask_length_mismatch_panics() {
        let mut arena = PeptideArena::new();
        arena.push("PEPTIDE");
        arena.retain(&[true, false]);
    }

    #[test]
    fn test_try_push_too_long() {
        let mut arena = PeptideArena::new();
        let too_long = "A".repeat(u16::MAX as usize + 1);
        assert!(arena.try_push(too_long).is_err());
    }
}
