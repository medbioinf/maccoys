// Purpose: main entry point for the web app
#![allow(non_snake_case)]

/// Main entry point for the web app
mod app;
/// Components used by the web app
mod components;
/// Entities used by the web app
mod entities;
/// Layouts for different pages
mod layouts;
/// Pages (render multiple compontents) used by the web app
mod pages;
/// Routes used by the web app
mod routes;

// 3rd party imports
use dioxus::prelude::LaunchBuilder;
use log::LevelFilter;

// internal import
use crate::app::App;

fn main() {
    // Init debug
    dioxus_logger::init(LevelFilter::Info).expect("failed to init logger");
    let cfg = dioxus::desktop::Config::new().with_custom_head(
        r#"
        <link rel="stylesheet" href="/index.css">
        <script src="/index.js"></script>
        "#
        .to_string(),
    );
    LaunchBuilder::desktop().with_cfg(cfg).launch(App);
}
