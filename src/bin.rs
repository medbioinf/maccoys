// std imports
use std::collections::HashMap;
use std::fs::{read_to_string, write as write_file};
use std::path::{Path, PathBuf};
use std::{env, process};

// 3rd party imports
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dihardts_omicstools::mass_spectrometry::spectrum::Spectrum as SpectrumTrait;
use dihardts_omicstools::proteomics::io::mzml::reader::Spectrum;
use dihardts_omicstools::proteomics::io::mzml::{
    index::Index, indexed_reader::IndexedReader, indexer::Indexer, reader::Reader as MzmlReader,
};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use glob::glob;
use maccoys::pipeline::configuration::{PipelineConfiguration, RemoteEntypointConfiguration};
use maccoys::pipeline::local_pipeline::LocalPipeline;
use maccoys::pipeline::remote_pipeline::RemotePipeline;
use maccoys::pipeline::remote_pipeline_web_api::RemotePipelineWebApi;
use maccoys::pipeline::tasks::cleanup_task::CleanupTask;
use maccoys::pipeline::tasks::identification_task::IdentificationTask;
use maccoys::pipeline::tasks::indexing_task::IndexingTask;
use maccoys::pipeline::tasks::preparation_task::PreparationTask;
use maccoys::pipeline::tasks::scoring_task::ScoringTask;
use maccoys::pipeline::tasks::search_space_generation_task::SearchSpaceGenerationTask;
use macpepdb::io::post_translational_modification_csv::reader::Reader as PtmReader;
use macpepdb::mass::convert::to_int as mass_to_int;
use metrics_exporter_prometheus::PrometheusBuilder;
use reqwest::Url;
use tokio::fs::OpenOptions;
use tokio::net::TcpListener;
use tracing::{debug, info, Level};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// internal imports
use maccoys::database::database_build::DatabaseBuild;
use maccoys::functions::{create_search_space, post_process};
use maccoys::io::comet::configuration::Configuration as CometConfiguration;
use maccoys::pipeline::queue::{LocalPipelineQueue, RedisPipelineQueue};
use maccoys::pipeline::storage::{LocalPipelineStorage, RedisPipelineStorage};
use maccoys::web::decoy_api::Server as DecoyApiServer;
use maccoys::web::results_api::Server as ResultApiServer;

/// Target for tracing
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum TracingTarget {
    Loki,
    File,
    Terminal,
    All,
}

/// Target for metrics
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum MetricTarget {
    Terminal,
    Prometheus,
    All,
}

/// Log rotation values for CLI
///
#[derive(clap::ValueEnum, Clone, Debug)]
enum TracingLogRotation {
    Minutely,
    Hourly,
    Daily,
    Never,
}

impl From<TracingLogRotation> for Rotation {
    fn from(rotation: TracingLogRotation) -> Self {
        match rotation {
            TracingLogRotation::Minutely => Rotation::MINUTELY,
            TracingLogRotation::Hourly => Rotation::HOURLY,
            TracingLogRotation::Daily => Rotation::DAILY,
            TracingLogRotation::Never => Rotation::NEVER,
        }
    }
}

#[derive(Debug, Subcommand)]
enum WebCommand {
    /// API for caching generated decoys
    DecoyApi {
        /// Interface where the service is spawned
        interface: String,
        /// Port where the service is spawned
        port: u16,
        /// Database URL for a MaCPepDB-like database for just for decoys
        database_url: String,
    },
    /// API for accessing the search results
    ResultApi {
        interface: String,
        port: u16,
        result_dir: PathBuf,
    },
}

/// CLI for the different web APIs
#[derive(Debug, Parser)]
struct WebCLI {
    #[command(subcommand)]
    command: WebCommand,
}

#[derive(Debug, Subcommand)]
enum PipelineCommand {
    /// Prints a new condition to stdout
    NewConfig {},
    /// Runs the full pipline locally.
    ///
    LocalRun {
        /// Use redis, make sure the Redis URL is set in the configs
        /// (makes only sense for debugging and testing when running locally)
        #[arg(short, long, default_value = "false")]
        use_redis: bool,
        /// Optional temp folder for intermediate files, default: `<system temp folder>/maccoys`
        #[arg(short, long)]
        tmp_dir: Option<PathBuf>,
        /// PTM file path
        #[arg(short, long)]
        ptms_file: Option<PathBuf>,
        /// Result directroy where each MS run will get a subdirectory
        result_dir: PathBuf,
        /// Path to the configuration file
        config: PathBuf,
        /// Default comet params
        default_comet_params_file: PathBuf,
        /// Paths to mzML files
        /// Glob patterns are allowed. e.g. /path/to/**/*.mzML, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        mzml_file_paths: Vec<String>,
    },
    /// Add the given MS runs to the remote pipeline
    /// Use `search-monitor` after this to monitor the search
    RemoteRun {
        /// PTM file path
        #[arg(short, long)]
        ptms_file: Option<PathBuf>,
        /// Base url to the entrypoin, e.g. http://localhost:8080
        base_url: String,
        /// Search parameters file path, contains `search_parameters`-section of the pipeline configuration
        search_parameters_file: PathBuf,
        /// Comet params
        comet_params_file: PathBuf,
        /// Paths to mzML files
        /// Glob patterns are allowed. e.g. /path/to/**/*.mzML, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        mzml_file_paths: Vec<String>,
    },
    /// Starts a web service which can be made public to submit data to be processed
    RemoteEntrypoint {
        /// Interface where the service is spawned
        interface: String,
        /// Port where the service is spawned
        port: u16,
        /// Directory where each search is stored
        work_dir: PathBuf,
        /// Contains `index`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Monitor for remote searches
    SearchMonitor {
        /// Contains `search_parameters`, `index`-, & `storages`-section of the pipeline configuration
        base_url: String,
        /// Search UUID
        uuid: String,
    },
    /// Standalone indexing for distributed processing
    Index {
        /// Work directroy where each MS run will get a subdirectory
        work_dir: PathBuf,
        /// Path to the indexing configuration file.
        /// Contains `index`-, `preparation`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone preparation for distributed processing
    Preparation {
        /// Path to the indexing configuration file.
        /// Contains `preparation`-, `search_space_generation`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone search space generation for distributed processing
    SearchSpaceGeneration {
        /// Path to the indexing configuration file.
        /// Contains `search_space_generation`-, `comet_search`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone comet search for distributed processing
    CometSearch {
        /// Optional temp folder for intermediate files, default: `<system temp folder>/maccoys`
        #[arg(short, long)]
        tmp_dir: Option<PathBuf>,
        /// Path to the indexing configuration file.
        /// Contains `comet_search`-, `goodness_and_rescoring`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone goodness and rescoring for distributed processing
    GoodnessAndRescoring {
        /// Path to the indexing configuration file.
        /// Contains `comet_search`-, `goodness_and_rescoring`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
    /// Standalone cleanup for distributed processing
    Cleanup {
        /// Work directroy where each MS run will get a subdirectory
        work_dir: PathBuf,
        /// Path to the indexing configuration file.
        /// Contains `goodness_and_rescoring`-, `cleanup`- & `storages`-section of the pipeline configuration
        config_file_path: PathBuf,
    },
}

#[derive(Debug, Parser)]
struct PipelineCLI {
    #[command(subcommand)]
    command: PipelineCommand,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Starts one of the various web APIs
    Web(WebCLI),
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
    /// Calculates the goodness of fit and the the exponential and distances scores for the given PSM file.
    PostProcess {
        /// Path to PSM file
        psm_file_path: String,
        /// Path to goodness of fit output file
        goodness_of_fit_file_path: String,
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
    /// How to log tracing. Can be used multiple times
    #[arg(short, long, value_enum, action = clap::ArgAction::Append)]
    tracing_target: Vec<TracingTarget>,
    /// Tracing log file. Only used if `file` is set in `tracing_target`.
    #[arg(short, long, default_value = "./logs/macpepdb.log")]
    file: PathBuf,
    /// Tracing log rotation. Only used if `file` is set in `tracing_target`.
    #[arg(short, long, value_enum, default_value = "never")]
    rotation: TracingLogRotation,
    /// Remote address of Loki endpoint for logging.
    /// Only used if `loki` is set in `tracing_target`.
    #[arg(short, long, default_value = "127.0.0.1:3100")]
    loki: String,
    /// How to log metrics. Can be used multiple times
    #[arg(short, long, value_enum, action = clap::ArgAction::Append)]
    metric_target: Vec<MetricTarget>,
    /// Local address to serve the Prometheus metrics endpoint.
    /// Port zero will automatically use a free port.
    /// Only used if `prometheus` is set in `metric_target`.
    #[arg(short, long, default_value = "127.0.0.1:9494")]
    prometheus: String,
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    //// Set up tracing
    let verbosity = match args.verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(verbosity.into())
        .add_directive("scylla=error".parse().unwrap())
        .add_directive("tokio_postgres=error".parse().unwrap())
        .add_directive("hyper=error".parse().unwrap())
        .add_directive("reqwest=error".parse().unwrap());

    // Tracing layers
    let mut tracing_indicatif_layer = None;
    let mut tracing_terminal_layer = None;
    let mut tracing_loki_layer = None;
    let mut tracing_file_layer = None;

    // Tracing guards/tasks
    let mut _tracing_loki_task = None;
    let mut _tracing_log_writer_guard = None;

    if args.tracing_target.contains(&TracingTarget::Terminal)
        || args.metric_target.contains(&MetricTarget::Terminal)
        || args.tracing_target.contains(&TracingTarget::All)
        || args.metric_target.contains(&MetricTarget::All)
    {
        let layer = IndicatifLayer::new()
            .with_span_child_prefix_symbol("\t")
            .with_span_child_prefix_indent("");
        tracing_indicatif_layer = Some(layer);
    }

    if args.tracing_target.contains(&TracingTarget::Terminal)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let parent_layer = tracing_indicatif_layer.as_ref().unwrap();
        tracing_terminal_layer =
            Some(tracing_subscriber::fmt::layer().with_writer(parent_layer.get_stderr_writer()));
    }

    if args.tracing_target.contains(&TracingTarget::File)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let file_appender = RollingFileAppender::new(
            args.rotation.into(),
            args.file.parent().unwrap(),
            args.file.file_name().unwrap(),
        );
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        tracing_file_layer = Some(tracing_subscriber::fmt::layer().with_writer(non_blocking));
        _tracing_log_writer_guard = Some(guard);
    }

    if args.tracing_target.contains(&TracingTarget::Loki)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let (layer, task) = tracing_loki::builder()
            .label("macpepdb", "development")?
            .extra_field("pid", format!("{}", process::id()))?
            .build_url(Url::parse(&format!("http://{}", args.loki)).unwrap())?;
        tracing_loki_layer = Some(layer);
        _tracing_loki_task = Some(tokio::spawn(task));
    }

    tracing_subscriber::registry()
        .with(tracing_terminal_layer)
        .with(tracing_indicatif_layer)
        .with(tracing_file_layer)
        .with(tracing_loki_layer)
        .with(filter)
        .init();

    //// Setup (prometheus) metrics

    let mut prometheus_scrape_address = None;

    if args.metric_target.contains(&MetricTarget::Prometheus)
        || args.metric_target.contains(&MetricTarget::Terminal)
        || args.metric_target.contains(&MetricTarget::All)
    {
        let prometheus_metrics_builder = PrometheusBuilder::new();

        // Create TCP listener for Prometheus metrics to check if port is available.
        // When Port 0 is given, the OS will choose a free port.
        let prometheus_scrape_socket_tmp = TcpListener::bind(&args.prometheus)
            .await
            .context("Creating TCP listener for Prometheus scrape endpoint")?;
        // Copy socket address to be able to use it for the endpoint
        let prometheus_scrape_socket = prometheus_scrape_socket_tmp.local_addr()?;
        prometheus_scrape_address = Some(format!(
            "{}:{}",
            prometheus_scrape_socket.ip(),
            prometheus_scrape_socket.port()
        ));
        // Drop listener to make port available for the web server
        drop(prometheus_scrape_socket_tmp);

        prometheus_metrics_builder
            .with_http_listener(prometheus_scrape_socket)
            .install()?;
    }

    info!("Welcome to MaCoyS - The `S` is silent!");

    match args.command {
        Commands::Web(web_command) => match web_command.command {
            WebCommand::DecoyApi {
                interface,
                port,
                database_url,
            } => {
                DecoyApiServer::start(&database_url, interface, port).await?;
            }
            WebCommand::ResultApi {
                interface,
                port,
                result_dir,
            } => {
                ResultApiServer::start(interface, port, result_dir).await?;
            }
        },
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

            let fasta_file = OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(true)
                .append(false)
                .open(fasta_file_path)
                .await
                .context("Error when opening FASTA file")?;
            let mut fasta_file = Box::pin(fasta_file);

            create_search_space(
                &mut fasta_file,
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
            let index = Indexer::create_index(spectra_file, chunks_size)?;
            let mut indexed_reader = IndexedReader::new(spectra_file, &index)?;
            let mut ms2_spectra_map: HashMap<String, (usize, usize)> = HashMap::new();
            for (spec_id, spec_offsets) in index.get_spectra() {
                if let Spectrum::MsNSpectrum(spec) = MzmlReader::parse_spectrum_xml(
                    indexed_reader.get_raw_spectrum(spec_id)?.as_slice(),
                )? {
                    if spec.get_ms_level() == 2 {
                        ms2_spectra_map.insert(spec_id.to_owned(), *spec_offsets);
                    }
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
            let index = Index::from_json(&read_to_string(Path::new(&index_file_path))?)?;
            let mut extractor =
                IndexedReader::new(Path::new(&original_spectrum_file_path), &index)?;
            write_file(
                Path::new(&output_file_path),
                extractor.extract_spectrum(&spectrum_id)?,
            )?;
        }
        Commands::PostProcess {
            psm_file_path,
            goodness_of_fit_file_path,
        } => {
            post_process(
                Path::new(&psm_file_path),
                Path::new(&goodness_of_fit_file_path),
            )
            .await?
        }
        Commands::Pipeline(pipeline_command) => match pipeline_command.command {
            PipelineCommand::NewConfig {} => {
                let new_config = PipelineConfiguration::new();
                println!("{}", toml::to_string_pretty(&new_config)?);
            }
            PipelineCommand::LocalRun {
                use_redis,
                tmp_dir,
                ptms_file,
                result_dir,
                config,
                default_comet_params_file,
                mzml_file_paths,
            } => {
                let tmp_dir = match tmp_dir {
                    Some(tmp_dir) => tmp_dir,
                    None => env::temp_dir().join("maccoys"),
                };

                debug!("Temp folder: {}", tmp_dir.display());

                let config: PipelineConfiguration =
                    toml::from_str(&read_to_string(&config).context("Reading config file")?)
                        .context("Deserialize config")?;

                let mut ptms: Vec<PostTranslationalModification> = Vec::new();
                if let Some(ptms_file) = &ptms_file {
                    ptms = PtmReader::read(Path::new(ptms_file))?;
                }

                let comet_config = match CometConfiguration::try_from(&default_comet_params_file) {
                    Ok(config) => config,
                    Err(e) => {
                        bail!("Error reading Comet configuration: {:?}", e);
                    }
                };

                let mzml_file_paths = convert_str_paths_and_resolve_globs(mzml_file_paths)?;
                if !use_redis {
                    info!("Running local pipeline");
                    LocalPipeline::<LocalPipelineQueue, LocalPipelineStorage>::run(
                        result_dir,
                        tmp_dir,
                        config,
                        comet_config,
                        ptms,
                        mzml_file_paths,
                        prometheus_scrape_address,
                    )
                    .await?;
                } else {
                    info!("Running redis pipeline");
                    LocalPipeline::<RedisPipelineQueue, RedisPipelineStorage>::run(
                        result_dir,
                        tmp_dir,
                        config,
                        comet_config,
                        ptms,
                        mzml_file_paths,
                        prometheus_scrape_address,
                    )
                    .await?;
                }
            }
            PipelineCommand::RemoteRun {
                ptms_file,
                base_url,
                search_parameters_file,
                comet_params_file,
                mzml_file_paths,
            } => {
                let mzml_file_paths = convert_str_paths_and_resolve_globs(mzml_file_paths)?;

                info!("Enqueue into remote pipeline");
                let uuid = RemotePipeline::run(
                    base_url,
                    search_parameters_file,
                    comet_params_file,
                    mzml_file_paths,
                    ptms_file,
                )
                .await?;
                println!("Do not forget your UUID: {}", uuid);
            }
            PipelineCommand::RemoteEntrypoint {
                interface,
                port,
                work_dir,
                config_file_path,
            } => {
                let config: RemoteEntypointConfiguration = toml::from_str(
                    &read_to_string(&config_file_path).context("Reading config file")?,
                )
                .context("Deserialize config")?;

                RemotePipelineWebApi::start(interface, port, work_dir, config).await?;
            }
            PipelineCommand::SearchMonitor { base_url, uuid } => {
                info!("Running search monitor");
                RemotePipeline::start_remote_search_monitor(base_url, &uuid).await?;
            }
            // PipelineCommand::QueueMonitor { base_url } => {
            //     info!("Running queue monitor");
            //     Pipeline::start_remote_queue_monitor(base_url).await?;
            // }
            // TODO: Is there a more generic way to implment the standalone tasks?
            // Only thing which changes is the configuration type and config attributes to call for the input and output queues
            // Except the cleanup task, which does not have an output queue
            PipelineCommand::Index {
                work_dir,
                config_file_path,
            } => IndexingTask::run_standalone(work_dir, config_file_path).await?,
            PipelineCommand::Preparation { config_file_path } => {
                PreparationTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::SearchSpaceGeneration { config_file_path } => {
                SearchSpaceGenerationTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::CometSearch {
                tmp_dir,
                config_file_path,
            } => {
                let tmp_dir = match tmp_dir {
                    Some(tmp_dir) => tmp_dir,
                    None => env::temp_dir().join("maccoys"),
                };
                IdentificationTask::run_standalone(tmp_dir, config_file_path).await?
            }
            PipelineCommand::GoodnessAndRescoring { config_file_path } => {
                ScoringTask::run_standalone(config_file_path).await?
            }
            PipelineCommand::Cleanup {
                work_dir,
                config_file_path,
            } => CleanupTask::run_standalone(work_dir, config_file_path).await?,
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
