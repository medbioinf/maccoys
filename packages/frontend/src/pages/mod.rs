/// Renders the MsRun page
mod ms_run;
/// 404 page
mod not_found;
/// Renders a search page
mod search;
/// Renders the spectrum and the identification results
mod spectrum;
/// Start page for selecting a project
mod start;

// reexport
pub use ms_run::MsRun;
pub use not_found::NotFound;
pub use search::Search;
pub use spectrum::Spectrum;
pub use start::Start;
