// 3rd party imports
use dioxus::prelude::*;

pub fn Start() -> Element {
    rsx! {
        input {
            r#type: "file",
            // Select a folder by setting the directory attribute
            directory: true,
            onchange: move |evt| {
                if let Some(file_engine) = evt.files() {
                    let files = file_engine.files();
                    for file_name in files {
                        println!("{}", file_name);
                    }
                }
            }
        }
    }
}
