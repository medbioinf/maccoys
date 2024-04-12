// 3rd party import
use dioxus::prelude::*;

// internal imports
use crate::layouts::two_panes::TwoPanes;
use crate::pages::*;

#[derive(Routable, PartialEq, Clone)]
#[rustfmt::skip]
pub enum Routes {
    #[layout(TwoPanes)]
        #[route("/")]
        Start,
    #[end_layout]
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}
