entrypoint: cargo run -r -- -vv -t terminal -t file --file ./logs/00-entrypoint.log --rotation never pipeline remote-entrypoint 127.0.0.1 6666 ./maccoys.local.toml
indexing: cargo run -r -- -vv -t terminal -t file --file ./logs/10-indexing.log --rotation never pipeline index ./maccoys.local.toml
search-space-generation: cargo run -r -- -vv -t terminal -t file --file ./logs/20-search-space-gen.log --rotation never pipeline search-space-generation ./maccoys.local.toml 
identification: cargo run -r -- -vv -t terminal -t file --file ./logs/30-identification.log --rotation never pipeline identification ./maccoys.local.toml 
scoring: cargo run -r -- -vv -t terminal -t file --file ./logs/40-scoring.log --rotation never pipeline scoring ./maccoys.local.toml 
publication:  cargo run -r -- -vv -t terminal -t file --file ./logs/50-publication.log --rotation never pipeline publication ./maccoys.local.toml 
error:  cargo run -r -- -vv -t terminal -t file --file ./logs/60-error.log --rotation never pipeline error ./maccoys.local.toml 
