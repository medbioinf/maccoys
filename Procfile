entrypoint: cargo run -r -- -vv --file ./logs/00-entrypoint.log --rotation never pipeline remote-entrypoint 127.0.0.1 6666 ./test_results ./maccoys.local.toml
indexing: cargo run -r -- -vv --file ./logs/10-indexing.log --rotation never pipeline index ./test_results ./maccoys.local.toml
search-space-generation: cargo run -r -- -vv --file ./logs/30-ss-gen.log --rotation never pipeline search-space-generation ./maccoys.local.toml 
identification: cargo run -r -- -vv --file ./logs/40-comet.log --rotation never pipeline identification ./maccoys.local.toml 
scoring: cargo run -r -- -vv --file ./logs/50-post.log --rotation never pipeline scoring ./maccoys.local.toml 
cleanup:  cargo run -r -- -vv --file ./logs/60-cleanup.log --rotation never pipeline publication ./test_results ./maccoys.local.toml 