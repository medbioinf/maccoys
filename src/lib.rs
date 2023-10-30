// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]
// Nightly for now as it is required for MaCPepDB
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]

/// Collection of constants
pub mod constants;
/// Module for database operations.
pub mod database;
/// Various functions for converting, parsing and generating data.
pub mod functions;
/// IO module for reading and writing files.
pub mod io;
/// Module for generating MaCcoyS search spaces
pub mod search_space;
/// Web API endpoints for adding decoys to a MaCPepDB database used only for decoys.
pub mod web;
