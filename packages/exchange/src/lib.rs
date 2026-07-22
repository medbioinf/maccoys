// Include readme in doc
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]

/// Entites for the results API
pub mod results_api;

/// Tools and extensions for different crates
pub mod tools;
