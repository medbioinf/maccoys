/// Endpoints for accessing results
mod results_controller;
/// Re-export the results controller
pub use results_controller::ResultController;
/// Server of the results API
pub mod server;
/// Re-export the results API server
pub use server::Server;
