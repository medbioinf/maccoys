queue: target/release/maccoys -vv --terminal --log-file ./logs/00-indexing.log --log-rotation never pipeline queue-server 127.0.0.1 8100 ./local.http.toml
entrypoint: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/10-entrypoint.log --log-rotation never pipeline remote-entrypoint 127.0.0.1 8101 ./local.http.toml
indexing: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/20-indexing.log --log-rotation never pipeline index ./local.http.toml
processing: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/30-processing.log --log-rotation never pipeline processing ./local.http.toml
finalization: cargo run -r --package maccoys -- -vv --terminal --log-file ./logs/60-finalization.log --log-rotation never pipeline finalization ./local.http.toml
