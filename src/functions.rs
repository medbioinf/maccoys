// std imports
use std::cmp::min;

// internal imports
use crate::constants::FASTA_SEQUENCE_LINE_LENGTH;

/// Creates a FASTA entry for the given sequence.
///
/// # Arguments
/// * `sequence` - Sequence
/// * `index` - Sequence index
/// * `entry_prefix` - Entry prefix
/// * `entry_name_prefix` - Entry name prefix
///
pub fn gen_fasta_entry(
    sequence: &str,
    index: usize,
    entry_prefix: &str,
    entry_name_prefix: &str,
) -> String {
    let mut entry = format!(
        ">{}|{entry_name_prefix}{index}|{entry_name_prefix}{index}\n",
        entry_prefix,
        entry_name_prefix = entry_name_prefix,
        index = index
    );
    for start in (0..sequence.len()).step_by(FASTA_SEQUENCE_LINE_LENGTH) {
        let stop = min(start + FASTA_SEQUENCE_LINE_LENGTH, sequence.len());
        entry.push_str(&sequence[start..stop]);
        entry.push('\n');
    }
    return entry;
}
