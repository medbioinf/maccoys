# MaCcoyS - Mass Centric Decoy Search
**The `S` is silent.**

MaCcoyS creates a spectrum specific search space with all available targets matching the precursor(s) of a given MS2 spectrum by fetching them from MaCPepDB. The MS2 spectrum in question is then identified using [Comet](https://uwpr.github.io/Comet/) and the specialized search space.    
The PSM validation is not relying on the FDR as usual but on a newly introduced hyper score. Therefore the exponential distribution is fitted to the PSM distribution per spectrum. If the PSM distribution is fitting well, this is tested using the \[???\] test, cumulative distribution function is used to calculate a survival score for each spectrum which is the new hyperscore.   
TODO: Separating of PSM


## Dependencieas
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

### Create a mzML index
`cargo run -r -- index-spectrum-file --help` will give you an overview what is necessary to identify a MS2 spectra.

Example:
```
cargo run -r -- index-spectrum-file ./mzmls/QExHF06833std.mzML  ./tmp/QExHF06833std.index.json
```

### Run searches
`cargo run -r -- search --help` will give you an overview what is necessary to identify a MS2 spectra.

Example:
```
cargo run -r -- search ./mzmls/QExHF06833std.mzML ./tmp/QExHF06833std.index.json 'controllerType=0 controllerNumber=1 scan=28374' ./tmp test_files/comet.params 5 5 3 6 10  scylla://localhost:9042/macpepdb_mouse -p ../macpepdb-rs/test_files/mods.csv
```