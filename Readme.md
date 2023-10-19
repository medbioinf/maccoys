# MaCcoyS - Mass Centric Decoy Search
The `S` is silent.


## Dependencieas
* GIT
* Rust nightly >= 2023-07-12

## Usage

### Create database cache
Create a cache for decoys.   
`cargo run -r -- database-build scylla://<COMMA_SEPARATED_LIST_OF_DB_NODES> <KEYSPACE> <URL_OR_FILEPATH_TO_MACPEPDB_CONFIG>`   
e.g.   
`cargo run -r -- database-build scylla://127.0.0.1:9042 maccoys http://127.0.0.1:9999/api/configuration`

### Run the decoy insertion endpoint
This endpoint is needed for caching decoys created for the identification.   
`cargo run -r web scylla://scylla://<COMMA_SEPARATED_LIST_OF_DB_NODES> <KEYSPACE> <INTERFACE/IP> <PORT>`   
e.g.   
`cargo run -r web scylla://localhost:9042 maccoys 127.0.0.1 10000`

### Run searches
Comming soon