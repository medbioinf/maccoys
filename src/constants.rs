/// Prefix for targets in FASTA files
///
pub const FASTA_TARGET_ENTRY_PREFIX: &str = "mdb";

/// Prefix for target names in FASTA files
///
pub const FASTA_TARGET_ENTRY_NAME_PREFIX: &str = "T";

/// Prefix for deciys in FASTA files
///
pub const FASTA_DECOY_ENTRY_PREFIX: &str = "moy";

/// Prefix for decoy names in FASTA files
///
pub const FASTA_DECOY_ENTRY_NAME_PREFIX: &str = "D";

/// Max length for sequence lines in FASTA files
///
pub const FASTA_SEQUENCE_LINE_LENGTH: usize = 60;

/// Max number of PSMs for the comet search
///
pub const COMET_MAX_PSMS: u32 = 10000;

/// Comet PSMs file header row
///
pub const COMET_HEADER_ROW: u8 = 1;

/// Comet PSMs file cell separator
///
pub const COMET_SEPARATOR: &str = "\t";

/// Comet base score for exponential score
///
pub const COMET_EXP_BASE_SCORE: &str = "xcorr";

/// Name for new exponential score
///
pub const EXP_SCORE_NAME: &str = "exp_score";

/// Comet base score for distance score
///
pub const COMET_DIST_BASE_SCORE: &str = COMET_EXP_BASE_SCORE;

/// Name for new distance score
///
pub const DIST_SCORE_NAME: &str = "dist_score";

/// FDR col name
///
pub const FDR_COL_NAME: &str = "fdr";
