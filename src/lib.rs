// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]
// Nightly for now as it is required for MaCPepDB

/// Module for database operations.
pub mod database;
/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
