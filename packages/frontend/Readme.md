# MaCcoyS Frontend

MaCcoyS only provides a web API for accessing the data. This is a separate frontend to make accessing the results much easier.


## Technical details
This frontend is build using [Dioxus](https://dioxuslabs.com/), which uses Rust to render and manage the DOM, handling any data and compiles into a WASM application. It is similar to React or VueJS.   
Another benefit of Dioxus is also the possibility to build an Electron Desktop App if necessary.

## Installation
1. Clone the repositry
2. Install rustup
3. Install [sass](https://sass-lang.com/install/), make sure it is available in your `PATH` as `sass`
4. Install Dioxus: `cargo install dioxus-cli`
5. Install WASM toolchain: `rustup target add wasm32-unknown-unknown`
6. `dx serve`

Per default the application uses the configuration `frontend.config.toml`. If your want to adjust it:
1. Make a copy of `frontend.config.toml` and adjust it
2. Run: `env MACCOYS_CONFIG=<PATH_TO_NEW_CONFIG> dx serve`

## Configuration
While a WASM application has no access to any filesystem, the configuration is compiled into the binary. Therefore it needs to be selected during compiling.

## Deplyoment
1. `dx build --release`
2. Serve the created `dist` folder with any web server, e.g. NginX



## Development
dx serve --platform web