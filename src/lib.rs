// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]
// Nightly for now as it is required for MaCPepDB
#![feature(async_fn_in_trait)]

/// Module for database operations.
pub mod database;
/// Module for generating MaCcoyS search spaces
pub mod search_space;
/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
