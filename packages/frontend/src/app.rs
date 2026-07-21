// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::{configuration::Configuration, routes::Routes};

/// Root component for the entire frontend
///
pub fn App() -> Element {
    use_context_provider(|| Configuration::new());
    
    rsx! { Router::<Routes> {} }
}
