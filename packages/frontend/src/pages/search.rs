// 3rd party imports
use dioxus::prelude::*;
use maccoys_exchange_entities::results_api::Search;

// internal imports
use crate::{configuration::Configuration, routes::Routes};

#[component]
pub fn Search(search_uuid: String) -> Element {
    let maccoys_base_url = use_context::<Configuration>()
        .get_maccoys_base_url()
        .to_string();
    let search_uuid_clone = search_uuid.clone();

    let search_future = use_resource(move || {
        let url = format!("{}/api/searches/{}", maccoys_base_url, search_uuid_clone);
        async move { reqwest::get(url).await.unwrap().json::<Search>().await }
    });

    rsx! {
        h2 { "Search: {search_uuid}" }
        match &*search_future.read_unchecked() {
            Some(Ok(search)) => rsx!{
                    for ms_run in search.get_ms_run_names().iter() {
                        li {
                            Link {
                                to: Routes::MsRun {
                                    search_uuid: search_uuid.clone(),
                                    ms_run_name: ms_run.clone(),
                                },
                                "{ms_run}"
                            }
                        }
                    }
            },
            Some(Err(_))=> rsx! {
                p { "Failed to load search" }
            },
            None => rsx! {
                p { "Loading search..." }
            }
        }
    }
}
