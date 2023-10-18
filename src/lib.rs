// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]
// Nightly for now as it is required for MaCPepDB

/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
