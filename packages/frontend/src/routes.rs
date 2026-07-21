// 3rd party import
use dioxus::prelude::*;

// internal imports
use crate::layouts::two_panes::TwoPanes;
use crate::pages::*;

#[derive(Routable, Clone)]
#[rustfmt::skip]
pub enum Routes {
    #[layout(TwoPanes)]
        #[route("/")]
        Start,
        #[nest("/searches")]
            #[nest("/:search_uuid")]
                #[route("")]
                Search { search_uuid: String },
                #[nest("/:ms_run_name")]
                    #[route("")]
                    MsRun { search_uuid: String, ms_run_name: String },
                    #[route("/:spectrum_id")]
                    Spectrum { search_uuid: String, ms_run_name: String, spectrum_id: String },
                #[end_nest]
            #[end_nest]    
        #[end_nest]
    #[end_layout]
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}
