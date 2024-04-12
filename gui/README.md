# MacPepDB GUI

Desktop ab for browsing results of MaCcoyS workflow.

## Development

The GUI is developed using Dioxus. CSS is written in SASS and is bundled along with necessary JS libraries with Webpack into `public/bunlde.js` & `public/bundle.css`. Both are than integrated into the Dioxus app.

### Requirements
* Rust development stack (Rustup, Cargo, ...)
* NodeJS
* yarn

### Structure
* `assets_raw` - all uncompiled/unbundled SASS and JS
* `assets` - ready to use assets (bundled JS & CSS, images, ...)
* `dist` - compiled Dioxus app


### Getting started
```shell
cd gui
yarn install
dx serve --hot-reload
```