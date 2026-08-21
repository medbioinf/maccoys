// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

/// Entites for the results API
pub mod results_api;
