entrypoint: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/00-entrypoint.log --log-rotation never pipeline remote-entrypoint 127.0.0.1 6666 /mnt/data/results/maccoys_test ./config.toml
index: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/10-indexing.log --log-rotation never pipeline index /mnt/data/results/maccoys_test ./config.toml
preparation: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/20-prep.log --log-rotation never pipeline preparation ./config.toml 
search-space-generation: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/30-ss-gen.log --log-rotation never pipeline search-space-generation ./config.toml 
comet-search: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/40-comet.log --log-rotation never pipeline comet-search ./config.toml 
goodness-and-rescoring: cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/50-post.log --log-rotation never pipeline goodness-and-rescoring ./config.toml 
cleanup:  cargo run -r --bin maccoys -- -vvvvvv --log-file ./logs/60-cleanup.log --log-rotation never pipeline cleanup /mnt/data/results/maccoys_test ./config.toml 