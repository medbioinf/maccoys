// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

/// Collection of constants
pub mod constants;
/// MaCcoyS specific errors
pub mod errors;
#[macro_use]
pub mod error;
/// IO module for reading and writing files.
pub mod io;
/// Arena-backed accumulator for peptide sequences
pub mod peptide_arena;
/// Peptide spectruim match
pub mod peptide_spectrum_match;
/// Pipeline for MaCcoyS searches
pub mod pipeline;
/// Precursor
pub mod precursor;
/// Module for generating MaCcoyS search spaces
pub mod search_space;
/// Utility funtions and extensions based on external crates.
pub mod utils;
/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
