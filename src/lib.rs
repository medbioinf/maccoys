// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

/// Collection of constants
pub mod constants;
/// Module for database operations.
pub mod database;
/// MaCcoyS specific errors
pub mod errors;
/// Basic functions necessary for running a MaCcoyS search.
pub mod functions;
/// Goodness of fit record for MaCcoyS searches.
pub mod goodness_of_fit_record;
/// IO module for reading and writing files.
pub mod io;
/// Peptide spectruim match
pub mod peptide_spectrum_match;
/// Pipeline for MaCcoyS searches
pub mod pipeline;
/// Precursor
pub mod precursor;
/// Module for generating MaCcoyS search spaces
pub mod search_space;
/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
