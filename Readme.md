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
cargo run -r -- search ./mzmls/QExHF06833std.mzML ./tmp/QExHF06833std.index.json 'controllerType=0 controllerNumber=1 scan=28374' ./tmp test_files/comet.params 5 5 3 0.02 0.0 6 10  scylla://localhost:9042/macpepdb_mouse -p ../macpepdb-rs/test_files/mods.csv
```

### Batch processing
For a identifying multiple mzMLs including all MS2 use the provided Nextflow workflow.

Example:
```
nextflow run -w ./tmp/work maccoys.nf --maccoys-bin $(pwd)/target/release/maccoys --mzml-dir ./tmp/mzmls --results-dir ./tmp/results --target-url scylla://localhost:9042/macpepdb_mouse --fragment-tolerance 0.02 --fragment-bin-offset 0.0
```

#### This has some additional requirements
* jq (Ubuntu: `apt install jq`, MacOS: `brew install jq`, Conda: `conda install jq`)
* [Comet](https://github.com/UWPR/Comet/releases)
    * Add it to your `PATH` environment variable or add a folder called `bin` next to `maccoys.nf`
* [Nextflow](https://www.nextflow.io/)

#### Arguments
Arguments are mostly equal to the MaCcoyS binary.

##### Required arguments
| Argument | Description |
| --- | --- |
| `--maccoys-bin` | MaCcoyS binary (need to be compiled previously) |
| `--spec-dir` | Directory with Thermo raw files (*.raw) or mzML files (*.mzml/*.mzML) |
| `--lower-mass-tol` | Lower mass tolerance of the MS (ppm) |
| `--upper-mass-tol` | Upper mass tolerance of the MS (ppm) |
| `--max-var-ptm` | Max. variable PTM per peptide |
| `--decoys-per-target` | Decoys per target |
| `--fragment-tolerance` | Fragment tolerance (for Comet `fragment_bin_tolerance`) |
| `--fragment-bin-offset` | Fragment bin offset (for Comet) |
| `--max-charge` | Max. charge tried for spectra where precurser charge was not reported. |
| `--target-url` | Web or database URL for fetching target from MaCPepDB |
| `--results-root-dir` | Empty directory for storing results. Directory name is also the name used in the web API / GUI |

##### Optional arguments
| Argument | Description |
| --- | --- |
| `--ptm-file` | Files defining PTMs |
| `--decoy-url` | `http` or `scylla` URL for fetching decoys |
| `--decoy-cache-url` | `http` or `scylla` URL for storing decoys |
| `--target-lookup-url` | `http`, `bloom+http` or `scylla` URL for target lookup |
| `--keep-search-files` | Set this to non-zero to keep the search files (search engine config, FASTA files) |  

#### Non-mzML files
The workflow is able to convert non-mzML file. This is done using Docker images, therefore the run command is a bit different: `nextflow run -profile conversion maccoys.nf ...`


##### Apple Silicon (M1, M2, ...)
```
env DOCKER_DEFAULT_PLATFORM=linux/amd64 nextflow run -profile docker maccoys.nf ...
```

TL;DR The used docker images for file conversion are not provided for ARM 64 CPUs, therefore Docker will print a warning on sdterr, which will stop the Nextflow process.



## Development

### Dependencies
* Rust nightly >= 2023-07-12
* [Nextflow](https://www.nextflow.io/)
* Conda | Mamba | Micromamba

### Preparation
1. Install Rust & Cargo. The easiest way is to install and use `rustup`
2. `rustup toolchain install nightly-2023-07-12`
3. Install Conda or one of it derivates
4. `conda env create -f environment.yaml`
5. `conda activate maccoys`
6. Install Nextflow