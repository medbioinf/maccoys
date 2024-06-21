// std imports
use std::collections::HashMap;
use std::fs::{read_to_string, write as write_file};
use std::path::{Path, PathBuf};

// 3rd party imports
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use dihardts_omicstools::mass_spectrometry::spectrum::Spectrum as SpectrumTrait;
use dihardts_omicstools::proteomics::io::mzml::reader::Spectrum;
use dihardts_omicstools::proteomics::io::mzml::{
    index::Index, indexed_reader::IndexedReader, indexer::Indexer, reader::Reader as MzmlReader,
};
use glob::glob;
use indicatif::ProgressStyle;
use maccoys::pipeline::{LocalPipelineQueue, Pipeline, PipelineConfiguration, RedisPipelineQueue};
use macpepdb::io::post_translational_modification_csv::reader::Reader as PtmReader;
use macpepdb::mass::convert::to_int as mass_to_int;
use tracing::{error, info, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// internal imports
use maccoys::database::database_build::DatabaseBuild;
use maccoys::functions::{create_search_space, post_process};
use maccoys::functions::{sanatize_string, search_preparation};
use maccoys::web::server::start as start_web_server;

#[derive(Debug, Subcommand)]
enum PipelineCommand {
    /// Prints a new condition to stdout
    NewConfig {},
    /// Runs the full pipline locally. Queuing is done in memory, unless the `piplines.redis_url` is set in the configuration (Redis + local run should only be used for testing).
    ///
    LocalRun {
        /// Path to the configuration file
        config: PathBuf,
        /// Paths to mzML files
        /// Glob patterns are allowed. e.g. /path/to/**/*.mzML, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        mzml_file_paths: Vec<String>,
    },
}

#[derive(Debug, Parser)]
struct PipelineCLI {
    #[command(subcommand)]
    command: PipelineCommand,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Web {
        database_url: String,
        interface: String,
        port: u16,
    },
    /// Build a MaCPepDB database for decoy caching.
    ///
    DatabaseBuild {
        /// Database URL of the format scylla://host1,host2,host3/keyspace
        database_url: String,
        /// Path or http(s)-URL to a MaCPepDB configuration JSON file. If a URL is given, the file will be downloaded and parsed.
        /// If you have no working MaCPepDB do download one, you can use the default from the MaCcoyS repo.
        configuration_resource: String,
    },
    /// Build search space for the given mass, mass tolerance and PTMs.
    ///
    SearchSpaceBuild {
        /// Path to search space FASTA file.
        fasta_file_path: String,
        /// Path to PTM file
        ptm_file_path: String,
        /// Mass
        mass: f64,
        /// Lower mass tolerance in PPM
        lower_mass_tolerance_ppm: i64,
        /// Upper mass tolerance in PPM
        upper_mass_tolerance_ppm: i64,
        /// Maximal number of variable modifications
        max_variable_modifications: i8,
        /// Amount of decoys to generate
        decoys_per_peptide: usize,
        /// URL for fetching targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        target_url: String,
        /// URL for decoys targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        #[arg(short)]
        decoy_url: Option<String>,
        /// Optional URL for checking generated decoy against targets.
        /// Can be a URL for the database (`scylla://host1,host2,host3/keyspace`),
        /// base url for MaCPepDB web API or base URL to MaCPepDB bloom filters (`bloom+http://<DOMAIN>`).
        /// If not given, decoys will not be checked against the target database and considered as "correct".
        #[arg(short)]
        target_lookup_url: Option<String>,
        /// URL for caching decoys, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        #[arg(short = 'c')]
        decoy_cache_url: Option<String>,
    },
    /// Index a spectrum file for fast access. Only mzML is supported yet.
    ///
    IndexSpectrumFile {
        /// Path to spectrum file
        spectrum_file_path: String,
        /// Path to index file
        index_file_path: String,
        /// Chunks to read from file at ones. Increase it if you have a lot on memory. Default: [crate::io::mzml::indexer::DEFAULT_CHUNK_SIZE]
        #[arg(short = 'c')]
        chunks_size: Option<usize>,
    },
    /// Extract a spectrum from a spectrum file into a separate valid file. Only mzML is supported yet.
    ExtractSpectrum {
        /// Path to original spectrum file
        original_spectrum_file_path: String,
        /// Path to index file for the original spectrum file
        index_file_path: String,
        /// Spectrum ID
        spectrum_id: String,
        /// Path to output file
        output_file_path: String,
    },
    /// Extracts the MS2 spectrum from the given mzML file and prepares the search for the given spectrum by
    /// creating the Comet configuration and FASTA files for each charge state.
    SearchPreparation {
        /// Path to original spectrum file
        original_spectrum_file_path: String,
        /// Path to index file for the original spectrum file
        index_file_path: String,
        /// Spectrum ID
        spectrum_id: String,
        /// Work directory
        work_dir: String,
        /// Path to default comet config file
        default_comet_file_path: String,
        /// Lower mass tolerance in PPM
        lower_mass_tolerance_ppm: i64,
        /// Upper mass tolerance in PPM
        upper_mass_tolerance_ppm: i64,
        /// Maximal number of variable modifications
        max_variable_modifications: i8,
        /// Fragment tolerance (for Comet fragment_bin_tol)
        fragment_tolerance: f64,
        /// Fragment bin tolerance offset (for Comet)
        fragment_bin_offset: f64,
        /// Charge limit used when spectrum has not charges assigned. Every charge from 1 to max_charge will be tried.
        max_charge: u8,
        /// Amount of decoys to generate
        decoys_per_peptide: usize,
        /// URL for fetching targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        target_url: String,
        /// URL for decoys targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        #[arg(short)]
        decoy_url: Option<String>,
        /// Optional URL for checking generated decoy against targets.
        /// Can be a URL for the database (`scylla://host1,host2,host3/keyspace`),
        /// base url for MaCPepDB web API or base URL to MaCPepDB bloom filters (`bloom+http://<DOMAIN>`).
        /// If not given, decoys will not be checked against the target database and considered as "correct".
        #[arg(short)]
        target_lookup_url: Option<String>,
        /// URL for caching decoys, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
        #[arg(short = 'c')]
        decoy_cache_url: Option<String>,
        /// Path to PTM file
        #[arg(short)]
        ptm_file_path: Option<String>,
    },
    /// Calculates the goodness of fit and the the exponential and distances scores for the given PSM file.
    PostProcess {
        /// Path to PSM file
        psm_file_path: String,
    },
    /// Sanitize spectrum ID (get rid of any special characters)
    SanitizeSpectrumId {
        /// Spectrum ID
        spectrum_id: String,
        /// If set, output does not contain a newline at the end
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        trim: bool,
    },
    /// MaCcoyS search pipeline
    Pipeline(PipelineCLI),
}

#[derive(Debug, Parser)]
#[command(name = "maccoys")]
struct Cli {
    /// Verbosity level
    /// 0 - Error
    /// 1 - Warn
    /// 2 - Info
    /// 3 - Debug
    /// > 3 - Trace
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    let verbosity = match args.verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(verbosity.into())
        .add_directive("scylla=info".parse().unwrap())
        .add_directive("tokio_postgres=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("reqwest=info".parse().unwrap())
        .add_directive("rustis=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new().with_progress_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {span_child_prefix}{span_name}{{{span_fields}}} {wide_msg} {elapsed}",
        )
        .unwrap()
    ).with_span_child_prefix_symbol("↳ ").with_span_child_prefix_indent(" ").with_max_progress_bars(10, None);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .with(filter)
        .init();

    info!("Welcome to MaCoyS - The `S` is silent!");

    match args.command {
        Commands::Web {
            database_url,
            interface,
            port,
        } => {
            if database_url.starts_with("scylla://") {
                start_web_server(&database_url, interface, port).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::DatabaseBuild {
            database_url,
            configuration_resource,
        } => {
            let build = DatabaseBuild::new(&database_url);
            build.build(&configuration_resource).await?;
            info!("Database build finished");
        }
        Commands::SearchSpaceBuild {
            fasta_file_path,
            ptm_file_path,
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            decoys_per_peptide,
            target_url,
            decoy_url,
            target_lookup_url,
            decoy_cache_url,
        } => {
            let ptms = PtmReader::read(Path::new(&ptm_file_path))?;
            create_search_space(
                Path::new(&fasta_file_path),
                &ptms,
                mass_to_int(mass),
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                decoys_per_peptide,
                target_url,
                decoy_url,
                target_lookup_url,
                decoy_cache_url,
            )
            .await?;
        }
        Commands::IndexSpectrumFile {
            spectrum_file_path,
            index_file_path,
            chunks_size,
        } => {
            let spectra_file = Path::new(&spectrum_file_path);
            let index = Indexer::create_index(&spectra_file, chunks_size)?;
            let mut indexed_reader = IndexedReader::new(&spectra_file, &index)?;
            let mut ms2_spectra_map: HashMap<String, (usize, usize)> = HashMap::new();
            for (spec_id, spec_offsets) in index.get_spectra() {
                match MzmlReader::parse_spectrum_xml(
                    indexed_reader.get_raw_spectrum(spec_id)?.as_slice(),
                )? {
                    Spectrum::MsNSpectrum(spec) => {
                        if spec.get_ms_level() == 2 {
                            ms2_spectra_map.insert(spec_id.clone(), spec_offsets.clone());
                        }
                    }
                    _ => {}
                }
            }
            let ms2_filtered_index = Index::new(
                index.get_file_path().clone(),
                index.get_indention().to_string(),
                index.get_default_data_processing_ref().to_string(),
                index.get_general_information_len(),
                ms2_spectra_map,
                HashMap::new(),
            );
            write_file(
                Path::new(&index_file_path),
                ms2_filtered_index.to_json()?.as_bytes(),
            )?;
        }
        Commands::ExtractSpectrum {
            original_spectrum_file_path,
            index_file_path,
            spectrum_id,
            output_file_path,
        } => {
            let index = Index::from_json(&read_to_string(&Path::new(&index_file_path))?)?;
            let mut extractor =
                IndexedReader::new(Path::new(&original_spectrum_file_path), &index)?;
            write_file(
                &Path::new(&output_file_path),
                extractor.extract_spectrum(&spectrum_id)?,
            )?;
        }
        Commands::SearchPreparation {
            original_spectrum_file_path,
            index_file_path,
            spectrum_id,
            work_dir,
            default_comet_file_path,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            fragment_tolerance,
            fragment_bin_offset,
            max_charge,
            decoys_per_peptide,
            target_url,
            decoy_url,
            target_lookup_url,
            decoy_cache_url,
            ptm_file_path,
        } => {
            let original_spectrum_file_path = Path::new(&original_spectrum_file_path);
            let index_file_path = Path::new(&index_file_path);
            let work_dir = Path::new(&work_dir);
            let default_comet_file_path = Path::new(&default_comet_file_path);
            let ptms = match ptm_file_path {
                Some(ptm_file_path) => PtmReader::read(Path::new(&ptm_file_path))?,
                None => Vec::new(),
            };

            search_preparation(
                &original_spectrum_file_path,
                &index_file_path,
                &spectrum_id,
                &work_dir,
                &default_comet_file_path,
                &ptms,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                fragment_tolerance,
                fragment_bin_offset,
                max_charge,
                decoys_per_peptide,
                &target_url,
                decoy_url,
                target_lookup_url,
                decoy_cache_url,
            )
            .await?;
        }
        Commands::PostProcess { psm_file_path } => post_process(&Path::new(&psm_file_path)).await?,
        Commands::SanitizeSpectrumId { spectrum_id, trim } => {
            if !trim {
                println!("{}", sanatize_string(&spectrum_id));
            } else {
                print!("{}", sanatize_string(&spectrum_id));
            }
        }
        Commands::Pipeline(pipeline_command) => match pipeline_command.command {
            PipelineCommand::NewConfig {} => {
                let new_config = PipelineConfiguration::new();
                println!("{}", toml::to_string_pretty(&new_config)?);
            }
            PipelineCommand::LocalRun {
                config,
                mzml_file_paths,
            } => {
                let config: PipelineConfiguration =
                    toml::from_str(&read_to_string(&config).context("Reading config file")?)
                        .context("Deserialize config")?;
                let mzml_file_paths = convert_str_paths_and_resolve_globs(mzml_file_paths)?;
                if config.pipelines.redis_url.is_none() {
                    info!("Running redis pipeline");
                    let pipline: Pipeline<LocalPipelineQueue> = Pipeline::new(config).await?;
                    pipline.run(mzml_file_paths).await?;
                } else {
                    info!("Running redis pipeline");
                    let pipline: Pipeline<RedisPipelineQueue> = Pipeline::new(config).await?;
                    pipline.run(mzml_file_paths).await?;
                }
            }
        },
    };

    Ok(())
}

/// Converts a vector of strings to a vector of paths and resolves glob patterns.
///
/// # Arguments
/// * `paths` - Vector of paths as strings
///
fn convert_str_paths_and_resolve_globs(paths: Vec<String>) -> Result<Vec<PathBuf>> {
    Ok(paths
        .into_iter()
        .map(|path| {
            if !path.contains("*") {
                // Return plain path in vecotor if no glob pattern is found
                Ok(vec![Path::new(&path).to_path_buf()])
            } else {
                // Resolve glob pattern and return array of paths
                Ok(glob(&path)?
                    .map(|x| Ok(x?))
                    .collect::<Result<Vec<PathBuf>>>()?)
            }
        })
        .collect::<Result<Vec<_>>>()? // Collect and resolve errors from parsing/resolving
        .into_iter()
        .flatten() // flatten the vectors which
        .collect())
}
