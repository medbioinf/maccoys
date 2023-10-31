/// Prefix for targets in FASTA files
///
pub const FASTA_TARGET_ENTRY_PREFIX: &'static str = "mdb";

/// Prefix for target names in FASTA files
///
pub const FASTA_TARGET_ENTRY_NAME_PREFIX: &'static str = "T";

/// Prefix for deciys in FASTA files
///
pub const FASTA_DECOY_ENTRY_PREFIX: &'static str = "moy";

/// Prefix for decoy names in FASTA files
///
pub const FASTA_DECOY_ENTRY_NAME_PREFIX: &'static str = "D";

/// Max length for sequence lines in FASTA files
///
pub const FASTA_SEQUENCE_LINE_LENGTH: usize = 60;

/// Max number of PSMs for the comet search
///
pub const COMET_MAX_PSMS: u32 = 10000;
