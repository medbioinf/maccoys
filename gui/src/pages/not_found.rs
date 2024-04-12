// 3rd party imports
use dioxus::prelude::*;

#[derive(PartialEq, Props, Clone)]
pub struct NotFoundProps {
    pub segments: Vec<String>,
}

pub fn NotFound(props: NotFoundProps) -> Element {
    rsx! {
        div {
            h1 { "404" }
            p { "Sorry, this page does not exists." }
        }
    }
}
