// std import
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    // Deal with assets
    let assets_path = Path::new("assets");
    // Bundle/compile SASS and JS files
    Command::new("yarn").arg("build").status().unwrap();

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");

    for entry in fs::read_dir(assets_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.to_str().unwrap());
    }
}
