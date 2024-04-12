// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::routes::Routes;

/// Root component for the entire frontend
///
pub fn App() -> Element {
    rsx! { Router::<Routes> {} }
}
