// Purpose: main entry point for the web app
#![allow(non_snake_case)]

/// Main entry point for the web app
mod app;
/// Components used by the web app
mod components;
/// App configuration
mod configuration;
/// Entities used by the web app
mod entities;
/// Layouts for different pages
mod layouts;
/// Pages (render multiple compontents) used by the web app
mod pages;
/// Routes used by the web app
mod routes;
/// Matomo tracking
mod tracking;

// 3rd party imports
use dioxus::prelude::*;
use dioxus_logger::tracing::Level;

// internal import
use crate::app::App;

fn main() {
    // Init debug
    dioxus_logger::init(Level::INFO).expect("failed to init logger");
    launch(App);
}
