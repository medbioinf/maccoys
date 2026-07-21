/// Re-export the decoy controller
pub use decoy_controller::DecoyController;
/// Controller with decoy endpoints
pub mod decoy_controller;
/// Server of the decoy API
pub mod server;
/// Re-export the decoy API server
pub use server::Server;
