queue: target/release/maccoys -vv --terminal --log-file ./logs/00-indexing.log --log-rotation never pipeline queue-server 127.0.0.1 8100 ./local.http.toml
entrypoint: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/10-entrypoint.log --log-rotation never pipeline remote-entrypoint 127.0.0.1 8101 ./local.http.toml
indexing: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/20-indexing.log --log-rotation never pipeline index ./local.http.toml
search-space-generation: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/30-search-space-gen.log --log-rotation never pipeline search-space-generation ./local.http.toml
identification: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/40-identification.log --log-rotation never pipeline identification ./local.http.toml
scoring: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/50-scoring.log --log-rotation never pipeline scoring ./local.http.toml
publication:  cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/60-publication.log --log-rotation never pipeline publication ./local.http.toml
error:  cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/70-error.log --log-rotation never pipeline error ./local.http.toml
