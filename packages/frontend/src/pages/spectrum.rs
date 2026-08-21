// 3rd party imports
use dioxus::prelude::*;
use maccoys_exchange_entities::results_api::Spectrum as SpecturmEntity;
use serde_json::json;

// internal imports
use crate::{configuration::Configuration, routes::Routes};

#[component]
pub fn Spectrum(search_uuid: String, ms_run_name: String, spectrum_id: String) -> Element {
    let spectrum_id = urlencoding::decode(&spectrum_id).unwrap().to_string();

    let maccoys_base_url = use_context::<Configuration>()
        .get_maccoys_base_url()
        .to_string();
    let search_uuid_clone = search_uuid.clone();
    let ms_run_name_clone = ms_run_name.clone();
    let spectrum_id_clone = spectrum_id.clone();

    let spectrum_feature = use_resource(move || {
        let url = format!(
            "{}/api/searches/{}/{}/{}",
            maccoys_base_url, search_uuid_clone, ms_run_name_clone, spectrum_id_clone
        );
        async move {
            reqwest::get(url)
                .await
                .unwrap()
                .json::<SpecturmEntity>()
                .await
        }
    });

    let spectrum_plot = document::eval(
        r#"
            let plot_data = await dioxus.recv();

            let data = [{
                x: plot_data.get("data_x"),
                y: plot_data.get("data_y"),
                type: plot_data.get("data_type"),
            }];


            let layout = {
                xaxis: {
                    title: plot_data.get("xaxis_title"),
                },
                yaxis: {
                    title: plot_data.get("yaxis_title"),
                },
            };

            Plotly.newPlot("specturm-plot", data, layout);
        "#,
    );

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
                li { class: "breadcrumb-item",
                    "MS Run: "
                    Link {
                        to: Routes::MsRun {
                            search_uuid: search_uuid.clone(),
                            ms_run_name: ms_run_name.clone(),
                        },
                        "{ms_run_name}"
                    }
                }
                li { class: "breadcrumb-item active", "Spectrum: {spectrum_id}" }
            }
        }

        h2 { "Spectrum: {spectrum_id}" }
        match &*spectrum_feature.read_unchecked() {
            Some(Ok(spectrum)) => {

                spectrum_plot.send(json!({
                    "data_x": spectrum.get_mz(),
                    "data_y": spectrum.get_intensity(),
                    "data_type": "bar",
                    "xaxis_title": "m/z",
                    "yaxis_title": "Intensity",
                })).unwrap();



                rsx!{
                    div {
                        id: "specturm-plot"
                    }
                    // add tab for each precurosr and load the PSMs on demand
                }
            },
            Some(Err(_))=> rsx! {
                p { "Failed to load spectrum" }
            },
            None => rsx! {
                p { "Loading spectrum..." }
            }
        }
    }
}
