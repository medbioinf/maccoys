// 3rd party imports
use dioxus::prelude::*;
use maccoys_exchange_entities::results_api::MsRun;

// internal imports
use crate::{configuration::Configuration, routes::Routes};

#[component]
pub fn MsRun(search_uuid: String, ms_run_name: String) -> Element {
    let maccoys_base_url = use_context::<Configuration>()
        .get_maccoys_base_url()
        .to_string();
    let search_uuid_clone = search_uuid.clone();
    let ms_run_clone = ms_run_name.clone();

    let ms_run_future = use_resource(move || {
        let url = format!(
            "{}/api/searches/{}/{}",
            maccoys_base_url, search_uuid_clone, ms_run_clone
        );
        async move { reqwest::get(url).await.unwrap().json::<MsRun>().await }
    });

    rsx! {
        nav {
            ul { class: "breadcrumb",
                li { class: "breadcrumb-item",
                    "Search: "
                    Link {
                        to: Routes::Search {
                            search_uuid: search_uuid.clone(),
                        },
                        "{search_uuid}"
                    }
                }
                li { class: "breadcrumb-item active", "MS Run {ms_run_name}" }
            }
        }

        h2 { "MS Run: {ms_run_name}" }
        match &*ms_run_future.read_unchecked() {
            Some(Ok(ms_run)) => rsx!{
                for spectrum_id in ms_run.get_spectra_ids().iter() {
                    li {
                        Link {
                            to: Routes::Spectrum {
                                search_uuid: search_uuid.clone(),
                                ms_run_name: ms_run_name.clone(),
                                spectrum_id: urlencoding::encode(spectrum_id).into_owned(),
                            },
                            "{spectrum_id}"
                        }
                    }
                }

            },
            Some(Err(_))=> rsx! {
                p { "Failed to load MS run" }
            },
            None => rsx! {
                p { "Loading MS run..." }
            }
        }
    }
}
