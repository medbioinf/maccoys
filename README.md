# MaCcoyS - Mass Centric Decoy Search
**The `S` is silent.**

MaCcoyS creates a spectrum specific search space with all available targets matching the precursor(s) of a given MS2 spectrum by fetching them from MaCPepDB. The MS2 spectrum in question is then identified using [Comet](https://uwpr.github.io/Comet/) and the specialized search space.    
The PSM validation is not relying on the FDR as usual but on a newly introduced hyper score. Therefore the exponential distribution is fitted to the PSM distribution per spectrum. If the PSM distribution is fitting well, this is tested using the \[???\] test, the cumulative distribution function is used to calculate a survival score for each spectrum which is the new hyperscore.   
TODO: Separating of PSM


## Installation

### Native
Coming as soon as published to `crates.io` and `pypy.org`. For now see section [Development](#development)

### Docker

#### Requirements
* [Docker](https://www.docker.com/)

#### Pull latest container
Comming as soon as published on quay.io. For now see section [Build latest](#build-latest-container)

#### Build latest container
1. Clone the repository
2. Run `docker build -t local/maccoys:latest -f docker/Dockerfile .` 


## Usage

### MS-file conversion
MaCcoyS is expecting the MS data in mzML format. Here is a list of tools to convert different vendor formats 
* `Thermo .raw-file`:
    * [msconvert of Prote Wizard](https://proteowizard.sourceforge.io/index.html) - Enable vendor peak picking. Windows users have GUI. Also available for Linux and Mac (Intel & Apple Silicon) users via docker: `docker run -it -e WINEDEBUG=-all -v $(pwd):/data chambm/pwiz-skyline-i-agree-to-the-vendor-licenses wine msconvert ...`
    * [ThermoRawFileParserGUI](http://compomics.github.io/projects/ThermoRawFileParserGUI) & [ThermoRawFileParser](https://github.com/compomics/ThermoRawFileParser) - Standard settings are fine. Not working on Apple Silicon.
* `Bruker .d-folders`:
    * (tdf2mzml)[https://github.com/mafreitas/tdf2mzml] - CLI only. Use `--ms1_type centroid`

### Batch processing
MaCcoyS has a integrated pipeline for batch processing which can be used locally or on distributed system.

#### Locally
Great for testing and playing around
1. Generate a comet config
    * native: `comet -p`
    * docker: `docker run --rm -it --entrypoint "" local/maccoys:dev bash -c 'comet -p > /dev/null; cat comet.params.new' > comet.params.new`
2. Generate a pipeline config
    * native: `maccoys pipeline new-config`
    * docker: `docker run --rm -it local/maccoys:dev pipeline new-config`
3. Adjust both configs to your MS-parameter and experimental design
4. Run the pipeline
    * native: `maccoys -vvvvvv -l <PATH_TO_LOG_FILE> pipeline local-run  <RESULT_FOLDER_PATH> <PATH_TO_MACCOYS_CONFIG_TOML> <PATH_TO_xcorr_config> <MZML_FILES_0> <MZML_FILE_1> ...`
    * docker: `docker run --rm -it local/maccoys:dev pipeline new-config`
5. `docker run --rm -it -v <ABSOLUTE_PATH_ON_HOST_>:/data local/maccoys:dev -vvvvvv -l /data/logs/maccoys.log pipeline local-run  /data/results /data/<PATH_TO_MACCOYS_CONFIG_TOML> /data/<PATH_TO_xcorr_config> /data/experiment/<MZML_FILES_0> /data/experiment/<MZML_FILE_1> ...`

Checkout `... pipline --help` and have a look on the optional parameter and the parameter descriptions which might be helpful.

#### Distributed
The pipeline can also be deployed on multiple machines. Have a look into `Procfile` do get an idea of the setup. The only requirement for the deployment is that each part of the pipeline has access to the results folder, e.g. via NFS, and has access to a central redis server.

You can test it via:
1. Shell 1: `docker compose up`
2. Shell 2: `ultraman start` (you can use any Procfile manager, like (ultraman)[https://github.com/yukihirop/ultraman], (foreman)[https://github.com/ddollar/foreman] or (honcho)[https://github.com/nickstenning/honcho/tree/main])

Using the following command, the search is send to the remote entrypoint and scheduled:
* native: `maccoys -vvvvvv pipeline remote-run <API_BASE_URL> <PATH_TO_SEARCH_PARAMETER_TOML> <PATH_TO_xcorr_config> <MZML_FILES_0> <MZML_FILE_1> ...`
* docker: `docker run --rm -it -v <ABSOLUTE_PATH_ON_HOST_>:/data local/maccoys:dev -vvvvvv pipeline local-run <API_BASE_URL> /data/<PATH_TO_SEARCH_PARAMETER_TOML> /data/<PATH_TO_xcorr_config> /data/experiment/<MZML_FILES_0> /data/experiment/<MZML_FILE_1> ...`

MaCcoyS will print an UUID to identify the search, e.g. to recheck the progress with `... -vvvvvvv pipeline search-monitor <API_BASE_URL> <UUID>` and find the results.

## Development

### Requirements
* Rust: The recommended way is to use [rustup](https://rustup.rs/)

### Preparation
1. `cargo build`, rustup should install the needed Rust version and compile
