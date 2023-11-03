# MaCcoyS - Mass Centric Decoy Search
**The `S` is silent.**

MaCcoyS creates a spectrum specific search space with all available targets matching the precursor(s) of a given MS2 spectrum by fetching them from MaCPepDB. The MS2 spectrum in question is then identified using [Comet](https://uwpr.github.io/Comet/) and the specialized search space.    
The PSM validation is not relying on the FDR as usual but on a newly introduced hyper score. Therefore the exponential distribution is fitted to the PSM distribution per spectrum. If the PSM distribution is fitting well, this is tested using the \[???\] test, cumulative distribution function is used to calculate a survival score for each spectrum which is the new hyperscore.   
TODO: Separating of PSM

## Installation
Coming as soon as published to `crates.io` and `pypy.org`. For now see section [Development](#development)

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

### Identify a single MS2 spectrum
`cargo run -r -- search --help` will give you an overview what is necessary to identify a MS2 spectra.

Example:
```
cargo run -r -- search ./mzmls/QExHF06833std.mzML ./tmp/QExHF06833std.index.json 'controllerType=0 controllerNumber=1 scan=28374' ./tmp test_files/comet.params 5 5 3 6 10  scylla://localhost:9042/macpepdb_mouse -p ../macpepdb-rs/test_files/mods.csv
```

### Batch processing
For a identifying multiple mzMLs including all MS2 use the provided Nextflow workflow.

Example:
```
nextflow run maccoys.nf --maccoys-bin $(pwd)/target/release/maccoys --mzml-dir ./mzmls --result-dir ./tmp --search-name test --target-url scylla://localhost:9042/macpepdb_mouse
```

Arguments are mostly equal to the MaCcoyS binary.

#### Required arguments
| Argument | Description |
| --- | --- |
| --maccoys-bin | MaCcoyS binary (need to be compiled previously) |
| --mzml-dir | Directory with mzML |
| --lower-mass-tol | Lower mass tolerance of the MS (ppm) |
| --upper-mass-tol | Upper mass tolerance of the MS (ppm) |
| --max-var-ptm | Max. variable PTM per peptide |
| --decoys-per-target | Decoys per target |
| --max-charge | Max. charge tried for spectra where precurser charge was not reported. |
| --target-url | Web or database URL for fetching target from MaCPepDB |
| --results-root-dir | Empty directory for storing results. Directory name is also the name used in the web API / GUI |

#### Required arguments
| Argument | Description |
| --- | --- |
| -- ptm-file | Files defining PTMs |


## Development

### Dependencies
* Rust nightly >= 2023-07-12
* Nextflow
* Conda | Mamba | Micromamba

### Preparation
1. Install Rust & Cargo. The easiest way is to install and use `rustup`
2. `rustup toolchain install nightly-2023-07-12`
3. Install Conda or one of it derivates
4. `conda env create -f environment.yaml`
5. `conda activate maccoys`
6. Install Nextflow